#include "KineticCluster.hh"
#include "Utility.hh"
#include <set>
#include <unistd.h>
#include "Logging.hh"
#include "outside/MurmurHash3.h"

using std::unique_ptr;
using std::shared_ptr;
using std::string;
using namespace kinetic;
using namespace kio;

KineticCluster::KineticCluster(
    std::size_t stripe_size, std::size_t num_parities, std::size_t block_size,
    std::vector<std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions> > info,
    std::chrono::seconds min_reconnect_interval,
    std::chrono::seconds op_timeout,
    std::shared_ptr<ErasureCoding> ec,
    SocketListener& listener
) : nData(stripe_size), nParity(num_parities), operation_timeout(op_timeout),
    clustersize{1, 0}, clustersize_background(1, 1), erasure(ec)
{
  if (nData + nParity > info.size()) {
    throw std::logic_error("Stripe size + parity size cannot exceed cluster size.");
  }

  /* Build connection vector */
  for (auto i = info.begin(); i != info.end(); i++) {
    std::unique_ptr<KineticAutoConnection> autocon(
        new KineticAutoConnection(listener, *i, min_reconnect_interval)
    );
    connections.push_back(std::move(autocon));
  }

  /* Attempt to get cluster limits from _any_ drive in the cluster */
  for (int off = 0; off < connections.size(); off++) {
    auto ops = initialize(std::make_shared<const string>("any"), 1, off);
    auto sync = asyncops::fillLog(ops, {Command_GetLog_Type::Command_GetLog_Type_LIMITS});
    auto rmap = execute(ops, *sync);
    if (rmap[StatusCode::OK]) {
      const auto& l = std::static_pointer_cast<GetLogCallback>(ops.front().callback)->getLog()->limits;
      if (l.max_value_size < block_size) {
        throw kio_exception(ENXIO, "configured block size of ", block_size,
                            "is smaller than maximum drive block size of ", l.max_value_size);
      }
      clusterlimits.max_key_size = l.max_key_size;
      clusterlimits.max_value_size = block_size * nData;
      clusterlimits.max_version_size = l.max_version_size;
      break;
    }
  }
  if (!clusterlimits.max_key_size || !clusterlimits.max_value_size || !clusterlimits.max_version_size) {
    throw kio_exception(ENXIO, "Failed obtaining cluster limits!");
  }
  /* Start a bg update of cluster capacity */
  size();
}

KineticCluster::~KineticCluster()
{
}

/* Build a stripe vector from the records returned by a get operation. Only
 * accept values with valid CRC. */
std::vector<shared_ptr<const string> > getOperationToStripe(
    std::vector<KineticAsyncOperation>& ops,
    int& count,
    const std::shared_ptr<const string>& target_version
)
{
  std::vector<shared_ptr<const string> > stripe;
  count = 0;

  for (auto o = ops.begin(); o != ops.end(); o++) {
    stripe.push_back(make_shared<const string>());
    auto& record = std::static_pointer_cast<GetCallback>(o->callback)->getRecord();

    if (record &&
        *record->version() == *target_version &&
        record->value() &&
        record->value()->size()
        ) {

      auto checksum = crc32c(0, record->value()->c_str(), record->value()->length());
      auto tag = utility::Convert::toString(checksum);
      if (tag == *record->tag()) {
        stripe.back() = record->value();
        count++;
      }
    }
  }
  return stripe;
}


KineticStatus KineticCluster::get(
    const shared_ptr<const string>& key,
    bool skip_value,
    shared_ptr<const string>& version,
    shared_ptr<const string>& value
)
{
  /* If we haven't encountered the need for parities during get in the last 10 minutes, try to get the value
   * without parities. */
  bool getParities = true;
  if (nData > nParity) {
    std::lock_guard<std::mutex> lock(mutex);
    if (std::chrono::duration_cast<std::chrono::minutes>(std::chrono::system_clock::now() - parity_required).count() >
        10) {
          getParities = false;
    }
  }

  auto ops = initialize(key, nData + (getParities ? nParity : 0));
  auto sync = skip_value ? asyncops::fillGetVersion(ops, key) : asyncops::fillGet(ops, key);
  auto rmap = execute(ops, *sync);

  auto target_version = skip_value ? asyncops::mostFrequentVersion(ops) : asyncops::mostFrequentRecordVersion(ops);
  rmap[StatusCode::OK] = target_version.frequency;

  /* Any status code encountered at least nData times is valid. If operation was a success, set return values. */
  for (auto it = rmap.cbegin(); it != rmap.cend(); it++) {
    if (it->second >= nData) {
      if (it->first == StatusCode::OK) {
        /* set value */
        if (!skip_value) {
          auto stripe_values = 0;
          auto stripe = getOperationToStripe(ops, stripe_values, target_version.version);

          /* stripe_values 0 -> empty value for key. */
          if (stripe_values == 0) {
            value = make_shared<const string>();
          }
          else {
            /* missing blocks -> erasure code. If we skipped reading parities, we have to abort. */
            if (stripe_values < stripe.size()) {
              if (!getParities) {
                break;
              }
              {
                std::lock_guard<std::mutex> lock(mutex);
                parity_required = std::chrono::system_clock::now();
              }
              try {
                erasure->compute(stripe);
              } catch (const std::exception& e) {
                return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, e.what());
              }
            }

            /* Create a single value from stripe values, around 0.5 ms per data subchunk. */
            auto v = make_shared<string>();
            v->reserve(stripe.front()->size() * nData);
            for (int i = 0; i < nData; i++) {
              *v += *stripe[i];
            }

            /* Resize value to size encoded in version (to support unaligned value sizes). */
            v->resize(utility::uuidDecodeSize(target_version.version));
            value = std::move(v);
          }
          /* set version */
          version = target_version.version;
        }
      }
      return KineticStatus(it->first, "");
    }
  }

  if (!getParities) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      parity_required = std::chrono::system_clock::now();
    }
    return get(key, skip_value, version, value);
  }

  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Key" + *key + "not accessible.");
}

static int getVersionPosition(const shared_ptr<const string>& version, std::vector<KineticAsyncOperation>& vops)
{
  for (int i = 0; i < vops.size(); i++) {
    if (vops[i].callback->getResult().ok() ||
        vops[i].callback->getResult().statusCode() == StatusCode::REMOTE_NOT_FOUND) {
      if (
          std::static_pointer_cast<GetVersionCallback>(vops[i].callback)->getVersion() == *version) {
        return i;
      }
    }
  }
  return -1;
}

/* Handle races, 2 or more clients attempting to put/remove the same key at the same time. */
bool KineticCluster::mayForce(const shared_ptr<const string>& key, const shared_ptr<const string>& version,
                              std::map<StatusCode, int, KineticCluster::compareStatusCode> ormap)
{
  if (ormap[StatusCode::OK] > (nData + nParity) / 2) {
    return true;
  }

  auto ops = initialize(key, nData + nParity);
  auto sync = asyncops::fillGetVersion(ops, key);
  auto rmap = execute(ops, *sync);
  auto most_frequent = asyncops::mostFrequentVersion(ops);

  if (rmap[StatusCode::REMOTE_NOT_FOUND] >= nData) {
    return version->size() != 0;
  }

  if (most_frequent.frequency && *version == *most_frequent.version) {
    return true;
  }

  if (most_frequent.frequency >= nData) {
    return false;
  }

  /* Super-Corner-Case: It could be that the client that should win the most_frequent match has crashed. For this
   * reason, all competing clients will wait, polling the key versions until a timeout. If the timeout expires
   * without the issue being resolved, overwrite permission will be given. As multiple clients
   * could theoretically be in this loop concurrently, we specify timeout time by the position of the first
   * occurence of the supplied version for a subchunk. */
  auto timeout_time = std::chrono::system_clock::now() + (getVersionPosition(version, ops) + 1) * operation_timeout;
  do {
    usleep(10000);
    auto ops = initialize(key, nData + nParity);
    auto sync = asyncops::fillGetVersion(ops, key);
    auto rmap = execute(ops, *sync);

    if (getVersionPosition(version, ops) < 0) {
      return false;
    }

    most_frequent = asyncops::mostFrequentVersion(ops);
    if (most_frequent.frequency >= nData || rmap[StatusCode::REMOTE_NOT_FOUND] >= nData) {
      return false;
    }
  } while (std::chrono::system_clock::now() < timeout_time);

  return true;
}

KineticStatus KineticCluster::put(
    const shared_ptr<const string>& key,
    const shared_ptr<const string>& version_in,
    const shared_ptr<const string>& value,
    bool force,
    shared_ptr<const string>& version_out)
{
  /* Create a stripe vector by chunking up the value into nData data chunks
     and computing nParity parity chunks. */
  int chunk_size = (value->size() + nData - 1) / (nData);
  std::vector<shared_ptr<const string> > stripe;
  for (int i = 0; i < nData + nParity; i++) {
    auto subchunk = std::make_shared<string>();
    if (i < nData) {
      if (i * chunk_size < value->size()) {
        subchunk->assign(value->substr(i * chunk_size, chunk_size));
      }
      subchunk->resize(chunk_size); // ensure that all chunks are the same size
    }
    stripe.push_back(std::move(subchunk));
  }
  try {
    /*Do not try to erasure code data if we are putting an empty key. The
      erasure coding would assume all chunks are missing. and throw an error.
      Computing takes about 4 ms for 8-2 and 60 ms for 38-4 */
    if (chunk_size) {
      erasure->compute(stripe);
    }
  } catch (const std::exception& e) {
    return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, e.what());
  }

  /* Do not use version_in, version_out variables directly in case the client
     supplies the same pointer for both. */
  auto version_old = version_in ? version_in : make_shared<const string>();
  auto version_new = utility::uuidGenerateEncodeSize(value->size());

  auto ops = initialize(key, nData + nParity);
  auto sync = asyncops::fillPut(
      ops, stripe, key, version_new, version_old,
      force ? WriteMode::IGNORE_VERSION : WriteMode::REQUIRE_SAME_VERSION
  );
  auto rmap = execute(ops, *sync);

  /* Partial stripe write has to be resolved. */
  if (rmap[StatusCode::OK] && (rmap[StatusCode::REMOTE_VERSION_MISMATCH] || rmap[StatusCode::REMOTE_NOT_FOUND])) {
    if (mayForce(key, version_new, rmap)) {
      auto forcesync = asyncops::fillPut(ops, stripe, key, version_new, version_new, WriteMode::IGNORE_VERSION);
      rmap = execute(ops, *forcesync);
    }
    else {
      return KineticStatus(StatusCode::REMOTE_VERSION_MISMATCH, "");
    }
  }

  for (auto it = rmap.cbegin(); it != rmap.cend(); it++) {
    if (it->second >= nData) {
      if (it->first == StatusCode::OK) {
        version_out = version_new;
      }
      return KineticStatus(it->first, "");
    }
  }
  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Key" + *key + "not accessible.");
}


KineticStatus KineticCluster::remove(
    const shared_ptr<const string>& key,
    const shared_ptr<const string>& version,
    bool force)
{
  auto ops = initialize(key, nData + nParity);
  auto sync = asyncops::fillRemove(
      ops, key, version, force ? WriteMode::IGNORE_VERSION : WriteMode::REQUIRE_SAME_VERSION
  );
  auto rmap = execute(ops, *sync);

  /* If we didn't find the key on a drive (e.g. because that drive was replaced)
   * we can just consider that key to be properly deleted on that drive. */
  if (rmap[StatusCode::OK] && rmap[StatusCode::REMOTE_NOT_FOUND]) {
    rmap[StatusCode::OK] += rmap[StatusCode::REMOTE_NOT_FOUND];
    rmap[StatusCode::REMOTE_NOT_FOUND] = 0;
  }

  /* Partial stripe remove has to be resolved */
  if (rmap[StatusCode::OK] && rmap[StatusCode::REMOTE_VERSION_MISMATCH]) {
    if (mayForce(key, std::make_shared<const string>(""), rmap)) {
      auto forcesync = asyncops::fillRemove(ops, key, version, WriteMode::IGNORE_VERSION);
      rmap = execute(ops, *forcesync);

      rmap[StatusCode::OK] += rmap[StatusCode::REMOTE_NOT_FOUND];
      rmap[StatusCode::REMOTE_NOT_FOUND] = 0;
    }
    else {
      rmap[StatusCode::REMOTE_VERSION_MISMATCH] += rmap[StatusCode::OK];
      rmap[StatusCode::OK] = 0;
    }
  }

  for (auto it = rmap.cbegin(); it != rmap.cend(); it++)
    if (it->second >= nData) {
      return KineticStatus(it->first, "");
    }

  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Key" + *key + "not accessible.");
}

std::map<kinetic::StatusCode, int, KineticCluster::compareStatusCode>  KineticCluster::getRangeKeys(std::vector<KeyLists>& klists, const std::shared_ptr<const std::string>& end_key)
{
  const int max_requests = 100;
  auto init_key = std::make_shared<const string>("getRangeKeyRequest");

  /* Build an operation vector containing an operation for every KeyList that has space */
  auto sync = std::unique_ptr<CallbackSynchronization>(new CallbackSynchronization());
  std::vector<KineticAsyncOperation> ops;
  for(int i=0; i<klists.size(); i++){
    auto& list = klists[i];
    bool list_requires_keys = (!list.keys || !list.next_keys) && *list.last_element != *end_key;

    if(list_requires_keys){
      auto op = initialize(init_key, 1, i);
      asyncops::fillRange(op, list.last_element, end_key, max_requests, sync);
      ops.push_back( op.front() );
    }
  }
  auto rmap = execute(ops, *sync);

  /* Move returned keys from operations to associated keylists */
  for(int i=0, o=0; i<klists.size(); i++){
    auto& list = klists[i];
    bool list_requires_keys = (!list.keys || !list.next_keys) && *list.last_element != *end_key;

    if(list_requires_keys){
      auto& opkeys = std::static_pointer_cast<RangeCallback>(ops[o++].callback)->getKeys();
      if (opkeys && opkeys->size()) {
        list.last_element = std::make_shared<const string>(opkeys->back() + " ");
        if (!list.keys)
          list.keys = std::move(opkeys);
        else
          list.next_keys = std::move(opkeys);
      }
      /* If we required keys but failed obtaining any, we put end_key in last_element to prevent retries */
      else
        list.last_element = end_key;
    }
  }
  return rmap;
}

KineticStatus KineticCluster::range(
    const std::shared_ptr<const std::string>& start_key,
    const std::shared_ptr<const std::string>& end_key,
    int requested,
    std::unique_ptr<std::vector<std::string> >& keys)
{
  bool need_keys = true;
  keys.reset(new std::vector<std::string>());
  std::vector<KeyLists> klists;
  for(int i=0; i<connections.size(); i++)
    klists.push_back(KeyLists(start_key));

  while (requested) {

    if(need_keys){
      auto rmap = getRangeKeys(klists, end_key);
      if(rmap[StatusCode::OK] < rmap.size() - nParity)
        return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Too many drives could not be accessed.");
      need_keys = false;
    }

    // find minimum front element
    std::string element(*end_key);
    for(auto it = klists.begin(); it != klists.end(); it++){
      if(it->keys && it->keys->at(it->position) < element)
        element = it->keys->at(it->position);
    }
    if(element == *end_key)
      break;

    // move position counter past element in all key lists, count frequency
    int element_frequency = 0;
    for (auto it = klists.begin(); it != klists.end(); it++) {
      if (it->keys && it->keys->at(it->position) == element) {
        element_frequency++;
        it->position++;
        if(it->position == it->keys->size()){
          it->keys = std::move(it->next_keys);
          it->position = 0;
          if(!it->keys)
            need_keys = true;
        }
      }
    }

    if (element_frequency >= nData) {
      keys->push_back(element);
      requested--;
    }
    else
      kio_debug("Key ", element, " has been read in only ", element_frequency, " times. Cluster repair might be advisable.");
  }
  return KineticStatus(StatusCode::OK, "");
}


void KineticCluster::updateSize()
{
  auto ops = initialize(make_shared<string>("all"), connections.size());
  auto sync = asyncops::fillLog(ops, {Command_GetLog_Type::Command_GetLog_Type_CAPACITIES});
  execute(ops, *sync);

  /* Evaluate Operation Result. */
  std::lock_guard<std::mutex> lock(mutex);
  clustersize.bytes_total = clustersize.bytes_free = 0;
  for (auto o = ops.begin(); o != ops.end(); o++) {
    if (!o->callback->getResult().ok()) {
      kio_notice("Could not obtain capacity information for a drive: ", o->callback->getResult());
      continue;
    }
    const auto& c = std::static_pointer_cast<GetLogCallback>(o->callback)->getLog()->capacity;
    clustersize.bytes_total += c.nominal_capacity_in_bytes;
    clustersize.bytes_free += c.nominal_capacity_in_bytes - (c.nominal_capacity_in_bytes * c.portion_full);
  }
}

const ClusterLimits& KineticCluster::limits() const
{
  return clusterlimits;
}

ClusterSize KineticCluster::size()
{
  clustersize_background.try_run(std::bind(&KineticCluster::updateSize, this));
  std::lock_guard<std::mutex> lock(mutex);
  return clustersize;
}

std::vector<KineticAsyncOperation> KineticCluster::initialize(
    const shared_ptr<const string> key,
    size_t size, off_t offset)
{
  std::uint32_t index;
  MurmurHash3_x86_32(key->c_str(), key->length(), 0, &index);
  index += offset;
  std::vector<KineticAsyncOperation> ops;

  while (size) {
    index = (index + 1) % connections.size();
    ops.push_back(
        KineticAsyncOperation{
            0,
            std::shared_ptr<kio::KineticCallback>(),
            connections[index].get()
        }
    );
    size--;
  }
  return ops;
}

std::map<StatusCode, int, KineticCluster::compareStatusCode> KineticCluster::execute(
    std::vector<KineticAsyncOperation>& ops,
    CallbackSynchronization& sync
)
{
  auto need_retry = false;
  auto rounds_left = 2;
  do {
    rounds_left--;
    std::vector<kinetic::HandlerKey> hkeys(ops.size());

    /* Call functions on connections */
    for (int i = 0; i < ops.size(); i++) {
      try {
        if (ops[i].callback->finished()) {
          continue;
        }
        auto con = ops[i].connection->get();
        hkeys[i] = ops[i].function(con);

        fd_set a;
        int fd;
        if (!con->Run(&a, &a, &fd)) {
          throw std::runtime_error("Connection::Run(...) returned false in KineticCluster::execute.");
        }
      }
      catch (const std::exception& e) {
        auto status = KineticStatus(StatusCode::CLIENT_IO_ERROR, e.what());
        ops[i].callback->OnResult(status);
        ops[i].connection->setError();
        kio_notice("Failed executing async operation ", i, " of ", ops.size(), " ", status);
      }
    }

    /* Wait until sufficient requests returned or we pass operation timeout. */
    std::chrono::system_clock::time_point timeout_time = std::chrono::system_clock::now() + operation_timeout;
    sync.wait_until(timeout_time);

    need_retry = false;
    for (int i = 0; i < ops.size(); i++) {
      /* timeout any unfinished request*/
      if (!ops[i].callback->finished()) {
        try {
          ops[i].connection->get()->RemoveHandler(hkeys[i]);
        } catch (...) { }
        kio_notice("Network timeout for operation ", i, " of ", ops.size());
        auto status = KineticStatus(KineticStatus(StatusCode::CLIENT_IO_ERROR, "Network timeout"));
        ops[i].callback->OnResult(status);
        ops[i].connection->setError();
      }

      /* Retry operations with CLIENT_IO_ERROR code result. Something went wrong with the connection,
       * we might just be able to reconnect and make the problem go away. */
      if (rounds_left && ops[i].callback->getResult().statusCode() == StatusCode::CLIENT_IO_ERROR) {
        ops[i].callback->reset();
        need_retry = true;
      }
    }
  } while (need_retry && rounds_left);

  std::map<kinetic::StatusCode, int, KineticCluster::compareStatusCode> map;
  for (auto it = ops.begin(); it != ops.end(); it++) {
    map[it->callback->getResult().statusCode()]++;
  }
  return map;
}

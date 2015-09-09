#include "KineticCluster.hh"
#include "Utility.hh"
#include <zlib.h>
#include <set>
#include <drive_log.h>
#include "Logging.hh"

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
    clustersize_background(1), erasure(ec)
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
      if(l.max_value_size < block_size) {
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
      /* validate the checksum, computing takes ~1ms per checksum */
      auto checksum = crc32(0,
                            (const Bytef*) record->value()->c_str(),
                            record->value()->length()
      );
      auto tag = std::to_string((long long unsigned int) checksum);
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
  auto ops = initialize(key, nData + nParity);
  auto sync = skip_value ? asyncops::fillGetVersion(ops, key) : asyncops::fillGet(ops, key);
  auto rmap = execute(ops, *sync);

  if (rmap[StatusCode::OK] >= nData) {
    if (skip_value) {
      auto target_version = asyncops::mostFrequentVersion(ops);
      if (target_version.frequency >= nData)
        version = target_version.version;
      rmap[StatusCode::OK] = target_version.frequency;
    }
    else {
      auto target_version = asyncops::mostFrequentRecordVersion(ops);
      auto stripe_values = 0;
      auto stripe = getOperationToStripe(ops, stripe_values, target_version.version);

      /* stripe_values 0 -> empty value for key. */
      if (stripe_values == 0) {
        value = make_shared<const string>();
        version = target_version.version;
        return KineticStatus(StatusCode::OK, "");
      }

      /* missing blocks -> erasure code */
      if (stripe_values < stripe.size()) {
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
      version = std::move(target_version.version);
    }
  }
  for (auto it = rmap.cbegin(); it != rmap.cend(); it++)
    if (it->second >= nData) {
      return KineticStatus(it->first, "");
    }
  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Confusion when attempting to get key" + *key);
}

/* handle races, 2 or more clients attempting to put/remove the same key at the same time */
bool KineticCluster::mayForce(const shared_ptr<const string>& key, const shared_ptr<const string>& version,
                              std::map<StatusCode, int, KineticCluster::compareStatusCode> rmap)
{
  if (rmap[StatusCode::OK] > (nData + nParity) / 2) {
    return true;
  }

  auto ops = initialize(key, nData + nParity);
  auto sync = asyncops::fillGetVersion(ops, key);
  execute(ops, *sync);
  auto most_frequent = asyncops::mostFrequentVersion(ops);

  return *version == *most_frequent.version;
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
    if (i < nData) {
      auto subchunk = make_shared<string>(value->substr(i * chunk_size, chunk_size));
      subchunk->resize(chunk_size); // ensure that all chunks are the same size
      stripe.push_back(std::move(subchunk));
    }
    else {
      stripe.push_back(make_shared<string>());
    }
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

  /* Error handling. If we just wrote a subset of the stripe, and one or more subchunks failed with not found or
   * version missmatch errors, attempt to overwrite them... if we are in a race with another client writing the
   * same stripe, we might have to abort and let the other client complete his put operation */

   if (rmap[StatusCode::OK] && rmap[StatusCode::OK] < nData + nParity
      && (rmap[StatusCode::REMOTE_VERSION_MISMATCH] || rmap[StatusCode::REMOTE_NOT_FOUND])) {

    if (mayForce(key, version_new, rmap)) {
      std::vector<shared_ptr<const string> > fstripe;
      std::vector<KineticAsyncOperation> fops;
      for(int i=0; i<ops.size(); i++){
        const auto& code = ops[i].callback->getResult().statusCode();
        if( code == StatusCode::REMOTE_VERSION_MISMATCH || code == StatusCode::REMOTE_NOT_FOUND){
          fops.push_back(initialize(key,1,i).front());
          fstripe.push_back(stripe[i]);
        }
      }
      auto fsync =  asyncops::fillPut(
          fops, fstripe, key, version_new, version_old, WriteMode::IGNORE_VERSION
      );
      auto fmap = execute(fops, *fsync);

      rmap[StatusCode::OK] += fmap[StatusCode::OK];
      rmap[StatusCode::REMOTE_VERSION_MISMATCH] = rmap[StatusCode::REMOTE_NOT_FOUND] = 0;
    }
    else {
      rmap[StatusCode::REMOTE_VERSION_MISMATCH] += rmap[StatusCode::OK] + rmap[StatusCode::REMOTE_NOT_FOUND];
      rmap[StatusCode::OK] = rmap[StatusCode::REMOTE_NOT_FOUND] = 0;
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
  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Confusion when attempting to put key" + *key);
}


KineticStatus KineticCluster::remove(
    const shared_ptr<const string>& key,
    const shared_ptr<const string>& version,
    bool force)
{
  auto ops = initialize(key, nData + nParity);
  auto sync = asyncops::fillRemove(ops, key, version,
                                   force ? WriteMode::IGNORE_VERSION : WriteMode::REQUIRE_SAME_VERSION
  );
  auto rmap = execute(ops, *sync);

  /* If we didn't find the key on a drive (e.g. because that drive was replaced)
   * we can just consider that key to be properly deleted on that drive. */
  if (rmap[StatusCode::OK] && rmap[StatusCode::REMOTE_NOT_FOUND]) {
    rmap[StatusCode::OK] += rmap[StatusCode::REMOTE_NOT_FOUND];
    rmap[StatusCode::REMOTE_NOT_FOUND] = 0;
  }

  if (rmap[StatusCode::OK] && rmap[StatusCode::OK] < nData + nParity && rmap[StatusCode::REMOTE_VERSION_MISMATCH]) {
    if (mayForce(key, std::make_shared<const string>(""), rmap)) {

      std::vector<KineticAsyncOperation> fops;
      for(int i=0; i<ops.size(); i++){
        const auto& code = ops[i].callback->getResult().statusCode();
        if( code == StatusCode::REMOTE_VERSION_MISMATCH ){
          fops.push_back(initialize(key,1,i).front());
        }
      }
      auto fsync =  asyncops::fillRemove(fops, key, version, WriteMode::IGNORE_VERSION);
      auto fmap = execute(fops, *fsync);

      rmap[StatusCode::OK] += fmap[StatusCode::OK];
      rmap[StatusCode::REMOTE_VERSION_MISMATCH] = 0;
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

  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Confusion when attempting to remove key" + *key);
}

KineticStatus KineticCluster::range(
    const std::shared_ptr<const std::string>& start_key,
    const std::shared_ptr<const std::string>& end_key,
    int maxRequested,
    std::unique_ptr<std::vector<std::string> >& keys)
{
  auto ops = initialize(start_key, connections.size());
  auto sync = asyncops::fillRange(ops, start_key, end_key, maxRequested);
  auto rmap = execute(ops, *sync);

  if (rmap[StatusCode::OK] > connections.size() - nData - nParity) {
    /* Process Results stored in Callbacks. */
    /* merge in set to eliminate doubles  */
    std::set<std::string> set;
    for (auto o = ops.cbegin(); o != ops.cend(); o++) {
      auto& opkeys = std::static_pointer_cast<RangeCallback>(o->callback)->getKeys();
      if(opkeys)
        set.insert(opkeys->begin(), opkeys->end());
    }

    /* assign to output parameter and cut excess results */
    keys.reset(new std::vector<std::string>(set.begin(), set.end()));
    if (keys->size() > maxRequested) {
      keys->resize(maxRequested);
    }
  }

  for (auto it = rmap.cbegin(); it != rmap.cend(); it++) {
    if (it->second > connections.size() - nData - nParity) {
      return KineticStatus(it->first, "");
    }
  }
  return KineticStatus(
      StatusCode::CLIENT_IO_ERROR,
      "Confusion when attempting to get range from key" + *start_key + " to " + *end_key
  );
}


void KineticCluster::updateSize()
{
  auto ops = initialize(make_shared<string>("all"), connections.size());
  auto sync = asyncops::fillLog(ops, {Command_GetLog_Type::Command_GetLog_Type_CAPACITIES});
  execute(ops, *sync);

  /* Evaluate Operation Result. */
  std::lock_guard<std::mutex> lock(clustersize_mutex);
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
  auto function = std::bind(&KineticCluster::updateSize, this);
  clustersize_background.try_run(function);

  std::lock_guard<std::mutex> lock(clustersize_mutex);
  return clustersize;
}

std::vector<KineticAsyncOperation> KineticCluster::initialize(
    const shared_ptr<const string> key,
    size_t size, off_t offset)
{
  std::size_t index = std::hash<std::string>()(*key) + offset;
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
          throw std::runtime_error("Connection unusable.");
        }
      }
      catch (const std::exception& e) {
        auto status = KineticStatus(StatusCode::CLIENT_IO_ERROR, e.what());
        ops[i].callback->OnResult(status);
        ops[i].connection->setError(status);
        kio_warning("Failed executing operation ", i, "of stripe. ", status);
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
        kio_warning("Network timeout for operation ", i, " of stripe.");
        auto status = KineticStatus(KineticStatus(StatusCode::CLIENT_IO_ERROR, "Network timeout"));
        ops[i].callback->OnResult(status);
        ops[i].connection->setError(status);
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

#include "KineticCluster.hh"
#include "LoggingException.hh"
#include "Utility.hh"
#include <zlib.h>
#include <set>


using std::chrono::system_clock;
using std::unique_ptr;
using std::shared_ptr;
using std::string;
using namespace kinetic;
using namespace kio;

/* return the frequency of the most common element in the vector, as well
   as a reference to that element. */
KineticAsyncOperation& mostFrequent(
    std::vector<KineticAsyncOperation>& ops, int& count,
    std::function<bool(const KineticAsyncOperation&, const KineticAsyncOperation&)> equal
)
{
  count = 0;
  auto element = ops.begin();

  for (auto o = ops.begin(); o != ops.end(); o++) {
    int frequency = 0;
    for (auto l = ops.begin(); l != ops.end(); l++)
      if (equal(*o, *l)) {
        frequency++;
      }

    if (frequency > count) {
      count = frequency;
      element = o;
    }
    if (frequency > ops.size() / 2) {
      break;
    }
  }
  return *element;
}

bool resultsEqual(const KineticAsyncOperation& lhs, const KineticAsyncOperation& rhs)
{
  return lhs.callback->getResult().statusCode() == rhs.callback->getResult().statusCode();
}

bool getVersionEqual(const KineticAsyncOperation& lhs, const KineticAsyncOperation& rhs)
{
  return
      std::static_pointer_cast<GetVersionCallback>(lhs.callback)->getVersion()
      ==
      std::static_pointer_cast<GetVersionCallback>(rhs.callback)->getVersion();
}

bool getRecordVersionEqual(const KineticAsyncOperation& lhs, const KineticAsyncOperation& rhs)
{
  if (!std::static_pointer_cast<GetCallback>(lhs.callback)->getRecord() ||
      !std::static_pointer_cast<GetCallback>(rhs.callback)->getRecord()) {
    return false;
  }

  return
      *std::static_pointer_cast<GetCallback>(lhs.callback)->getRecord()->version()
      ==
      *std::static_pointer_cast<GetCallback>(rhs.callback)->getRecord()->version();
}


KineticCluster::KineticCluster(
    std::size_t stripe_size, std::size_t num_parities,
    std::vector<std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions> > info,
    std::chrono::seconds min_reconnect_interval,
    std::chrono::seconds op_timeout,
    std::shared_ptr<ErasureCoding> ec,
    SocketListener& listener
) :
    nData(stripe_size), nParity(num_parities),
    connections(), operation_timeout(op_timeout),
    clusterlimits{0, 0, 0}, clustersize{0, 0},
    sizeStatus(StatusCode::CLIENT_INTERNAL_ERROR, "not initialized"),
    sizeOutstanding(false),
    mutex(), erasure(ec)
{
  if (nData + nParity > info.size()) {
    throw std::logic_error("Stripe size + parity size cannot exceed cluster size.");
  }

  for (auto i = info.begin(); i != info.end(); i++) {
    std::unique_ptr<KineticAutoConnection> ncon(
        new KineticAutoConnection(listener, *i, min_reconnect_interval)
    );
    connections.push_back(std::move(ncon));
  }

  /* Attempt to get cluster limits from _any_ drive in the cluster */
  for(int off =0; off <connections.size(); off++){
    auto ops = initialize(std::make_shared<const string>("any"), 1, off);
    auto sync = asyncop_fill::log(ops, {Command_GetLog_Type::Command_GetLog_Type_LIMITS});
    auto status = execute(sync, ops);
    if(status.ok()){
      const auto& l = std::static_pointer_cast<GetLogCallback>(ops.front().callback)->getLog()->limits;
      clusterlimits.max_key_size = l.max_key_size;
      clusterlimits.max_value_size = l.max_value_size * nData;
      clusterlimits.max_version_size = l.max_version_size;
      break;
    }
  }
  if(!clusterlimits.max_key_size || !clusterlimits.max_value_size || !clusterlimits.max_version_size)
    throw LoggingException(
        ENXIO, __FUNCTION__, __FILE__, __LINE__,
        "Failed obtaining cluster limits!"
    );

  updateSize();
}

KineticCluster::~KineticCluster()
{
  /* Ensure that no background getlog operation is running, as it will
     access member variables. */
  while (true) {
    std::lock_guard<std::mutex> lck(mutex);
    if (!sizeOutstanding) {
      break;
    }
  };
}

KineticStatus KineticCluster::getVersion(
    const std::shared_ptr<const std::string>& key,
    std::shared_ptr<const std::string>& version
)
{
  auto ops = initialize(key, nData + nParity);
  auto sync = asyncop_fill::getVersion(ops, key);
  auto status = execute(sync, ops);
  if (!status.ok()) return status;

  int count;
  auto& op = mostFrequent(ops, count, getVersionEqual);
  if (count < nData) {
    return KineticStatus(
        StatusCode::CLIENT_IO_ERROR,
        "Unreadable: " + std::to_string((long long int) count) +
        " equal versions does not reach read quorum of " +
        std::to_string((long long int) nData)
    );
  }
  version.reset(new string(std::move(
      std::static_pointer_cast<GetVersionCallback>(op.callback)->getVersion()
  )));
  return status;
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
  return std::move(stripe);
}

KineticStatus KineticCluster::get(
    const shared_ptr<const string>& key,
    bool skip_value,
    shared_ptr<const string>& version,
    shared_ptr<const string>& value
)
{
  if (skip_value) {
    return getVersion(key, version);
  }

  auto ops = initialize(key, nData + nParity);
  auto sync = asyncop_fill::get(ops, key);
  auto status = execute(sync, ops);
  if (!status.ok()) return status;

  /* At least nData get operations succeeded. Validate that a
   * read quorum of nData operations returned a conforming version. */
  int count = 0;
  auto& op = mostFrequent(ops, count, getRecordVersionEqual);
  if (count < nData) {
    return KineticStatus(
        StatusCode::CLIENT_IO_ERROR,
        "Unreadable: " + std::to_string((long long int) count) +
        " equal versions does not reach read quorum of " +
        std::to_string((long long int) nData)
    );
  }
  auto target_version = std::static_pointer_cast<GetCallback>(op.callback)->getRecord()->version();
  auto stripe = getOperationToStripe(ops, count, target_version);

  /* count 0 -> empty value for key. */
  if (count == 0) {
    value = make_shared<const string>();
    version = target_version;
    return status;
  }

  /* missing blocks -> erasure code */
  if (count < stripe.size()) {
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
  v->resize(utility::uuidDecodeSize(target_version));
  value = std::move(v);
  version = std::move(target_version);
  return status;
}


KineticStatus KineticCluster::put(
    const shared_ptr<const string>& key,
    const shared_ptr<const string>& version_in,
    const shared_ptr<const string>& value,
    bool force,
    shared_ptr<const string>& version_out)
{
  auto ops = initialize(key, nData + nParity);

  /* Do not use version_in, version_out variables directly in case the client
     supplies the same pointer for both. */
  auto version_old = version_in ? version_in : make_shared<const string>();
  auto version_new = utility::uuidGenerateEncodeSize(value->size());

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

  auto sync = asyncop_fill::put(ops, stripe, key, version_new, version_old,
                                force ? WriteMode::IGNORE_VERSION : WriteMode::REQUIRE_SAME_VERSION
  );
  auto status = execute(sync, ops);

  if (status.ok()) {
    version_out = version_new;
  }
  return status;
}


KineticStatus KineticCluster::remove(
    const shared_ptr<const string>& key,
    const shared_ptr<const string>& version,
    bool force)
{
  auto ops = initialize(key, nData + nParity);
  auto sync = asyncop_fill::remove(ops, key, version,
                                   force ? WriteMode::IGNORE_VERSION : WriteMode::REQUIRE_SAME_VERSION
  );
  return execute(sync, ops);
}

KineticStatus KineticCluster::range(
    const std::shared_ptr<const std::string>& start_key,
    const std::shared_ptr<const std::string>& end_key,
    int maxRequested,
    std::unique_ptr<std::vector<std::string> >& keys)
{
  auto ops = initialize(start_key, connections.size());
  auto sync = asyncop_fill::range(ops, start_key, end_key, maxRequested);
  auto status = execute(sync, ops);
  if (!status.ok()) return status;

  /* Process Results stored in Callbacks. */

  /* merge in set to eliminate doubles  */
  std::set<std::string> set;
  for (auto o = ops.cbegin(); o != ops.cend(); o++) {
    set.insert(
        std::static_pointer_cast<RangeCallback>(o->callback)->getKeys()->begin(),
        std::static_pointer_cast<RangeCallback>(o->callback)->getKeys()->end()
    );
  }

  /* assign to output parameter and cut excess results */
  keys.reset(new std::vector<std::string>(set.begin(), set.end()));
  if (keys->size() > maxRequested) {
    keys->resize(maxRequested);
  }

  return status;
}


void KineticCluster::updateSize()
{
  /* This function is executed in a background thread. Make sure it never
     ever throws anything, as otherwise the whole process terminates. */
  try {
    auto ops = initialize(make_shared<string>("all"), connections.size());
    auto sync = asyncop_fill::log(ops, {Command_GetLog_Type::Command_GetLog_Type_CAPACITIES});
    auto status = execute(sync, ops);

    /* Evaluate Operation Result. */
    std::lock_guard<std::mutex> lock(mutex);
    sizeStatus = status;
    sizeOutstanding = false;
    if (!sizeStatus.ok())
      return;

    /* Process Results stored in Callbacks. */
    clustersize = {0, 0};
    for (auto o = ops.begin(); o != ops.end(); o++) {
      if (!o->callback->getResult().ok())
        continue;
      const auto& c = std::static_pointer_cast<GetLogCallback>(o->callback)->getLog()->capacity;
      clustersize.bytes_total += c.nominal_capacity_in_bytes;
      clustersize.bytes_free += c.nominal_capacity_in_bytes - (c.nominal_capacity_in_bytes * c.portion_full);
    }
  } catch (...) { }
}

const ClusterLimits& KineticCluster::limits() const
{
  return clusterlimits;
}

KineticStatus KineticCluster::size(ClusterSize& size)
{
  std::lock_guard<std::mutex> lock(mutex);
  if (!sizeOutstanding) {
    sizeOutstanding = true;
    std::thread(&KineticCluster::updateSize, this).detach();
  }

  if (sizeStatus.ok()) {
    size = clustersize;
  }

  return sizeStatus;
}


std::vector<KineticAsyncOperation> KineticCluster::initialize(
    const shared_ptr<const string>& key,
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


KineticStatus KineticCluster::execute(
    std::shared_ptr<CallbackSynchronization>& sync,
    std::vector<KineticAsyncOperation>& ops)
{
  std::vector<kinetic::HandlerKey> hkeys;

  /* Call functions on connections */
  for (auto o = ops.begin(); o != ops.end(); o++) {
    try {
      auto con = o->connection->get();
      hkeys.push_back(o->function(con));

      fd_set a;
      int fd;
      if (!con->Run(&a, &a, &fd)) {
        throw std::runtime_error("Connection unusable.");
      }
    }
    catch (const std::exception& e) {
      o->callback->OnResult(
          KineticStatus(StatusCode::REMOTE_REMOTE_CONNECTION_ERROR, e.what())
      );
    }
  }

  /* Wait until sufficient requests returned or we pass operation timeout. */
  system_clock::time_point timeout_time = std::chrono::system_clock::now() + operation_timeout;
  sync->wait_until(timeout_time, 0);

  /* timeout any unfinished request, do not mark the associated connection
   * as broken, if Run() failed it has already been done, otherwise it is an
   * honest time out. */
  for (int i = 0; i < ops.size(); i++) {
    if (!ops[i].callback->finished()) {
      ops[i].connection->get()->RemoveHandler(hkeys[i]);
      ops[i].callback->OnResult(KineticStatus(StatusCode::CLIENT_IO_ERROR, "Network timeout"));
    }
  }

  /* In almost all cases this loop will terminate after a single iteration. */
  int count = 0;
  auto& op = mostFrequent(ops, count, resultsEqual);
  if (ops.size() >= nData && count < nData) {
    return KineticStatus(
        StatusCode::CLIENT_IO_ERROR,
        "Failed to get quorum of conforming return results from drives."
    );
  }
  return op.callback->getResult();
}

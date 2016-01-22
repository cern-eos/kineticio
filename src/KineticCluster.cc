/************************************************************************
 * KineticIo - a file io interface library to kinetic devices.          *
 *                                                                      *
 * This Source Code Form is subject to the terms of the Mozilla         *
 * Public License, v. 2.0. If a copy of the MPL was not                 *
 * distributed with this file, You can obtain one at                    *
 * https://mozilla.org/MP:/2.0/.                                        *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but is provided AS-IS, WITHOUT ANY WARRANTY; including without       *
 * the implied warranty of MERCHANTABILITY, NON-INFRINGEMENT or         *
 * FITNESS FOR A PARTICULAR PURPOSE. See the Mozilla Public             *
 * License for more details.                                            *
 ************************************************************************/

#include "KineticCluster.hh"
#include "Utility.hh"
#include <set>
#include <unistd.h>
#include "Logging.hh"
#include "KineticIoSingleton.hh"
#include "outside/MurmurHash3.h"

using std::unique_ptr;
using std::shared_ptr;
using std::string;
using namespace kinetic;
using namespace kio;

KineticCluster::KineticCluster(
    std::string id, std::size_t block_size, std::chrono::seconds op_timeout,
    std::vector<std::unique_ptr<KineticAutoConnection>> cons,
    std::shared_ptr<RedundancyProvider> rp_data,
    std::shared_ptr<RedundancyProvider> rp_metadata
) : identity(id), instanceIdentity(utility::uuidGenerateString()), chunkCapacity(block_size),
    operation_timeout(op_timeout), connections(std::move(cons)), dmutex(std::make_shared<DestructionMutex>())
{
  redundancy[KeyType::Data] = rp_data;
  redundancy[KeyType::Metadata] = rp_metadata;

  /* Attempt to get cluster limits from _any_ drive in the cluster */
  for (int off = 0; off < connections.size(); off++) {
    auto ops = initialize(std::make_shared<const string>("any"), 1, off);
    auto sync = asyncops::fillLog(ops, {Command_GetLog_Type::Command_GetLog_Type_LIMITS});
    auto rmap = execute(ops, *sync);

    if (rmap[StatusCode::OK]) {
      const auto& l = std::static_pointer_cast<GetLogCallback>(ops.front().callback)->getLog()->limits;
      if (l.max_value_size < block_size) {
        kio_error("block size of ", block_size, "is bigger than maximum drive block size of ", l.max_value_size);
        throw std::system_error(std::make_error_code(std::errc::invalid_argument));
      }

      ClusterLimits dl, mdl;
      dl.max_range_elements = mdl.max_range_elements = 100;
      dl.max_key_size = mdl.max_key_size = l.max_key_size;
      dl.max_version_size = mdl.max_version_size = l.max_version_size;
      dl.max_value_size = block_size * redundancy[KeyType::Data]->numData();
      mdl.max_value_size = block_size * redundancy[KeyType::Metadata]->numData();
      cluster_limits[KeyType::Data] = dl;
      cluster_limits[KeyType::Metadata] = mdl;
      break;
    }
    if (off == connections.size()) {
      kio_error("Failed obtaining cluster limits!");
      throw std::system_error(std::make_error_code(std::errc::io_error));
    }
  }

  /* Start a bg update of cluster io statistics and capacity */
  statistics_snapshot.bytes_total = 1;
  kio().threadpool().try_run(std::bind(&KineticCluster::updateStatistics, this, dmutex));
}

KineticCluster::~KineticCluster()
{
  dmutex->setDestructed();
}


const ClusterLimits& KineticCluster::limits(KeyType type) const
{
  return cluster_limits.at(type);
}

const std::string& KineticCluster::instanceId() const
{
  return instanceIdentity;
}

const std::string& KineticCluster::id() const
{
  return identity;
}

ClusterStats KineticCluster::stats()
{
  std::lock_guard<std::mutex> lock(mutex);
  using namespace std::chrono;
  if (duration_cast<seconds>(system_clock::now() - statistics_scheduled) > seconds(2)) {
    if (kio().threadpool().try_run(std::bind(&KineticCluster::updateStatistics, this, dmutex))) {
      statistics_scheduled = system_clock::now();
    }
  }
  return statistics_snapshot;
}


KineticStatus KineticCluster::range(const std::shared_ptr<const std::string>& start_key,
                                    const std::shared_ptr<const std::string>& end_key,
                                    std::unique_ptr<std::vector<std::string>>& keys, KeyType type)
{
  int max_keys = cluster_limits[type].max_range_elements;
  if (keys && keys->capacity() > 0 && keys->capacity() < max_keys) {
    max_keys = keys->capacity();
  }

  auto ops = initialize(start_key, connections.size());
  auto sync = asyncops::fillRange(ops, start_key, end_key, max_keys);
  auto rmap = execute(ops, *sync);

  if (rmap[StatusCode::OK] > connections.size() - redundancy[type]->size()) {
    /* merge in set to eliminate doubles */
    std::set<string> set;
    for (auto o = ops.cbegin(); o != ops.cend(); o++) {
      auto& opkeys = std::static_pointer_cast<RangeCallback>(o->callback)->getKeys();
      if (opkeys) {
        set.insert(opkeys->begin(), opkeys->end());
      }
    }

    /* assign to output parameter and cut excess results */
    keys.reset(new std::vector<string>(set.begin(), set.end()));
    if (keys->size() > max_keys) {
      keys->resize(max_keys);
    }
    else {
      keys->reserve(max_keys);
    }
  }

  for (auto it = rmap.cbegin(); it != rmap.cend(); it++) {
    if (it->second > connections.size() - redundancy[type]->size()) {
      kio_debug("Range request from key ", *start_key, " to ", *end_key, " completed with status: ", it->first);
      return KineticStatus(it->first, "");
    }
  }
  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Range failed from key" + *start_key + " to " + *end_key);
}

KineticStatus KineticCluster::do_remove(const std::shared_ptr<const std::string>& key,
                                        const std::shared_ptr<const std::string>& version,
                                        KeyType type, WriteMode wmode)
{
  auto ops = initialize(key, redundancy[type]->size());
  auto sync = asyncops::fillRemove(ops, key, version, wmode);
  auto rmap = execute(ops, *sync);

  /* If we didn't find the key on a drive (e.g. because that drive was replaced)
   * we can just consider that key to be properly deleted on that drive. */
  if (rmap[StatusCode::OK] && rmap[StatusCode::REMOTE_NOT_FOUND]) {
    rmap[StatusCode::OK] += rmap[StatusCode::REMOTE_NOT_FOUND];
    rmap[StatusCode::REMOTE_NOT_FOUND] = 0;
  }

  /* Partial stripe remove has to be resolved */
  if (rmap[StatusCode::OK] && rmap[StatusCode::REMOTE_VERSION_MISMATCH]) {
    if (mayForce(key, std::make_shared<const string>(""), rmap, type)) {
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

  for (auto it = rmap.cbegin(); it != rmap.cend(); it++) {
    if (it->second >= redundancy[type]->numData()) {
      if (it->second < redundancy[type]->size()) {
        putIndicatorKey(key, ops);
      }

      kio_debug("Remove request for key ", *key, " completed with status: ", it->first);
      return KineticStatus(it->first, "");
    }
  }
  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Key" + *key + "not accessible.");

}

KineticStatus KineticCluster::remove(const std::shared_ptr<const std::string>& key,
                                     const std::shared_ptr<const std::string>& version,
                                     KeyType type)
{
  return do_remove(key, version, type, WriteMode::REQUIRE_SAME_VERSION);
}

kinetic::KineticStatus KineticCluster::remove(const std::shared_ptr<const std::string>& key, KeyType type)
{
  auto version = make_shared<const string>();
  return do_remove(key, version, type, WriteMode::IGNORE_VERSION);
}


/* Fire and forget put callback... not doing anything with the result. */
class FFPutCallback : public kinetic::PutCallbackInterface {
public:
  void Success()
  { }

  void Failure(kinetic::KineticStatus error)
  { }

  FFPutCallback()
  { }

  ~FFPutCallback()
  { }
};

void KineticCluster::putIndicatorKey(const std::shared_ptr<const std::string>& key,
                                     const std::vector<KineticAsyncOperation>& ops)
{
  /* Obtain an operation index where the execution succeeded. */
  int i = -1;
  while (i + 1 < ops.size()) {
    if (ops[++i].callback->getResult().statusCode() != StatusCode::CLIENT_IO_ERROR) {
      break;
    }
  }

  /* Set up record, version is set to "indicator" so that multiple attempts to write the same indicator key
   * fail with VERSION_MISMATCH instead of overwriting every single time. */
  auto record = std::make_shared<const KineticRecord>(
      "", "indicator", "", com::seagate::kinetic::client::proto::Command_Algorithm_INVALID_ALGORITHM
  );

  /* Schedule a write... we're not sticking around. If something fails, tough luck. */
  try {
    auto con = ops[i].connection->get();
    con->Put(utility::makeIndicatorKey(*key),
             make_shared<const string>(),
             kinetic::WriteMode::REQUIRE_SAME_VERSION,
             record,
             std::make_shared<FFPutCallback>());

    fd_set a;
    con->Run(&a, &a, &i);
  } catch (const std::exception& e) {
    kio_warning("Failed scheduling indication-key write for target-key ", *key, ": ", e.what());
  };
}


std::vector<std::shared_ptr<const std::string>> KineticCluster::valueToStripe(const std::string& value, KeyType type)
{
  if (!value.length()) {
    return std::vector<std::shared_ptr<const string>>(redundancy[type]->size(), std::make_shared<const string>());
  }

  std::vector<std::shared_ptr<const string>> stripe;

  auto chunkSize = value.length() < chunkCapacity ? value.length() : chunkCapacity;
  auto zero = make_shared<string>();
  zero->resize(chunkSize);

  /* Set data chunks of the stripe. If value < stripe size, fill in with 0ed strings. */
  for (int i = 0; i < redundancy[type]->numData(); i++) {
    if (i * chunkSize < value.length()) {
      auto chunk = std::make_shared<string>(value.substr(i * chunkSize, chunkSize));
      chunk->resize(chunkSize);
      stripe.push_back(chunk);
    }
    else {
      stripe.push_back(zero);
    }
  }
  /* Set empty strings for parities */
  for (int i = 0; i < redundancy[type]->numParity(); i++) {
    stripe.push_back(std::make_shared<const string>());
  }
  /* Compute redundancy */
  redundancy[type]->compute(stripe);

  /* We don't actually want to write the 0ed data chunks used for redundancy computation. So get rid of them. */
  for (int index = (value.size() + chunkSize - 1) / chunkSize; index < redundancy[type]->numData(); index++) {
    stripe[index] = std::make_shared<const string>();
  }

  return stripe;
}

std::shared_ptr<const string> KineticCluster::getOperationToValue(
    const std::vector<KineticAsyncOperation>& ops,
    const std::shared_ptr<const string>& key,
    const std::shared_ptr<const string>& version,
    KeyType type
)
{
  auto size = (int) utility::uuidDecodeSize(version);
  if (!size) {
    return make_shared<string>();
  }

  std::vector<shared_ptr<const string>> stripe;

  auto chunkSize = size < chunkCapacity ? size : chunkCapacity;
  bool need_recovery = false;
  auto zero = make_shared<string>();
  zero->resize(chunkSize);

  /* Step 1) re-construct stripe */
  for (int i = 0; i < ops.size(); i++) {
    auto& record = std::static_pointer_cast<GetCallback>(ops[i].callback)->getRecord();

    if (record && *record->version() == *version && record->value()) {
      auto checksum = crc32c(0, record->value()->c_str(), record->value()->length());
      auto tag = utility::Convert::toString(checksum);
      if (tag == *record->tag()) {
        stripe.push_back(record->value());
      }
      else {
        putIndicatorKey(key, ops);
        kio_warning("Chunk ", i, " of key ", *key, " failed crc verification.");
        stripe.push_back(make_shared<const string>());
        need_recovery = true;
      }
    }
    else {
      kio_notice("Chunk ", i, " of key ", *key, " is invalid.");
      stripe.push_back(make_shared<const string>());
      need_recovery = true;
    }
  }

  if (need_recovery) {
    /* Add 0ed data chunks as necessary to allow for redundancy computation */
    for (int index = (size + chunkSize - 1) / chunkSize; index < redundancy[type]->numData(); index++) {
      stripe[index] = zero;
    }
    redundancy[type]->compute(stripe);
  }

  /* Step 2) merge data chunks into single value */
  auto value = make_shared<string>();
  value->reserve(size);
  for (auto it = stripe.cbegin(); it != stripe.cend(); it++) {
    if (value->size() + chunkCapacity < size) {
      *value += **it;
    }
    else {
      value->append(**it, 0, size - value->size());
      break;
    }
  }
  return value;
}

kinetic::KineticStatus KineticCluster::do_get(const std::shared_ptr<const std::string>& key,
                                              std::shared_ptr<const std::string>& version,
                                              std::shared_ptr<const std::string>& value, KeyType type, bool skip_value)
{
  /* Try to get the value without parities for erasure coded stripes, unless the cluster has been switched into parity
   * mode in the last 5 minutes. */
  bool getParities = true;
  if(redundancy[type]->numData() > 1)
  {
    std::lock_guard<std::mutex> lock(mutex);
    using namespace std::chrono;
    if (duration_cast<minutes>(system_clock::now() - parity_required) > minutes(5)) {
      getParities = false;
    }
  }

  auto ops = initialize(key, redundancy[type]->numData() + (getParities ? redundancy[type]->numParity() : 0));
  auto sync = skip_value ? asyncops::fillGetVersion(ops, key) : asyncops::fillGet(ops, key);
  auto rmap = execute(ops, *sync);

  auto target_version = skip_value ? asyncops::mostFrequentVersion(ops) : asyncops::mostFrequentRecordVersion(ops);

  /* Put down an indicator if chunk versions of this stripe are not aligned */
  if (rmap[StatusCode::OK] > target_version.frequency) {
    putIndicatorKey(key, ops);
  }
  rmap[StatusCode::OK] = target_version.frequency;

  /* Any status code encountered at least nData times is valid. If operation was a success, set return values. */
  for (auto it = rmap.begin(); it != rmap.end(); it++) {
    if (it->second >= redundancy[type]->numData()) {
      if (it->first == StatusCode::OK) {

        if (!skip_value) {  /* set value */
          try {
            value = getOperationToValue(ops, key, target_version.version, type);
          } catch (const std::exception& e) {
            /* If we didn't get parities, retry operation. */
            if (!getParities) {
              break;
            }
            kio_error("Failed reconstructing value for key ", *key, ": ", e.what());
            return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, e.what());
          }
        }
        /* set version after value so that it will not be set in case of an error */
        version = target_version.version;
      }

      kio_debug("Get", skip_value ? "Version" : "Data", " request for key ", *key, " completed with status: ",
                it->first);
      return KineticStatus(it->first, "");
    }
  }

  if (!getParities) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      parity_required = std::chrono::system_clock::now();
    }
    return do_get(key, version, value, type, skip_value);
  }

  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Key" + *key + "not accessible.");
}

kinetic::KineticStatus KineticCluster::get(const std::shared_ptr<const std::string>& key,
                                           std::shared_ptr<const std::string>& version, KeyType type)
{
  std::shared_ptr<const string> value;
  return do_get(key, version, value, type, true);
}

kinetic::KineticStatus KineticCluster::get(const std::shared_ptr<const std::string>& key,
                                           std::shared_ptr<const std::string>& version,
                                           std::shared_ptr<const std::string>& value, KeyType type)
{
  return do_get(key, version, value, type, false);
}

namespace {
int getVersionPosition(const shared_ptr<const string>& version, std::vector<KineticAsyncOperation>& vops)
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
}

/* Handle races, 2 or more clients attempting to put/remove the same key at the same time. */
bool KineticCluster::mayForce(const shared_ptr<const string>& key, const shared_ptr<const string>& version,
                              std::map<StatusCode, int, KineticCluster::compareStatusCode> ormap, KeyType type)
{
  if (ormap[StatusCode::OK] > redundancy[type]->size() / 2) {
    return true;
  }

  auto ops = initialize(key, redundancy[type]->size());
  auto sync = asyncops::fillGetVersion(ops, key);
  auto rmap = execute(ops, *sync);
  auto most_frequent = asyncops::mostFrequentVersion(ops);

  if (rmap[StatusCode::REMOTE_NOT_FOUND] >= redundancy[type]->numData()) {
    return version->size() != 0;
  }

  if (most_frequent.frequency && *version == *most_frequent.version) {
    return true;
  }

  if (most_frequent.frequency >= redundancy[type]->numData()) {
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
    ops = initialize(key, redundancy[type]->size());
    sync = asyncops::fillGetVersion(ops, key);
    rmap = execute(ops, *sync);

    if (getVersionPosition(version, ops) < 0) {
      return false;
    }

    most_frequent = asyncops::mostFrequentVersion(ops);
    if (most_frequent.frequency >= redundancy[type]->numData() ||
        rmap[StatusCode::REMOTE_NOT_FOUND] >= redundancy[type]->numData()) {
      return false;
    }
  } while (std::chrono::system_clock::now() < timeout_time);

  return true;
}

KineticStatus KineticCluster::do_put(const std::shared_ptr<const std::string>& key,
                                     const std::shared_ptr<const std::string>& version,
                                     const std::shared_ptr<const std::string>& value,
                                     std::shared_ptr<const std::string>& version_out,
                                     KeyType type,
                                     kinetic::WriteMode mode)
{
  /* Do not use version_in, version_out variables directly in case the client
     supplies the same pointer for both. */
  auto version_old = version ? version : make_shared<const string>();
  auto version_new = utility::uuidGenerateEncodeSize(value->size());
  std::vector<std::shared_ptr<const string>> stripe;
  try {
    stripe = valueToStripe(*value, type);
  } catch (const std::exception& e) {
    kio_error("Failed building data stripe for key ", *key, ": ", e.what());
    return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, e.what());
  }

  auto ops = initialize(key, redundancy[type]->size());
  auto sync = asyncops::fillPut(ops, stripe, key, version_new, version_old, mode);
  auto rmap = execute(ops, *sync);

  /* Partial stripe write has to be resolved. */
  if (rmap[StatusCode::OK] && (rmap[StatusCode::REMOTE_VERSION_MISMATCH] || rmap[StatusCode::REMOTE_NOT_FOUND])) {
    if (mayForce(key, version_new, rmap, type)) {
      auto forcesync = asyncops::fillPut(ops, stripe, key, version_new, version_new, WriteMode::IGNORE_VERSION);
      rmap = execute(ops, *forcesync);
    }
    else {
      kio_debug("Put request for key ", *key, " failed partial write resolution and completed with status: ",
                StatusCode::REMOTE_VERSION_MISMATCH);
      return KineticStatus(StatusCode::REMOTE_VERSION_MISMATCH, "");
    }
  }

  for (auto it = rmap.cbegin(); it != rmap.cend(); it++) {
    if (it->second >= redundancy[type]->numData()) {
      if (it->first == StatusCode::OK) {
        version_out = version_new;
      }
      if (it->second < redundancy[type]->size()) {
        putIndicatorKey(key, ops);
      }

      kio_debug("Put request for key ", *key, " completed with status: ", it->first);
      return KineticStatus(it->first, "");
    }
  }
  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Key" + *key + "not accessible.");
}

kinetic::KineticStatus KineticCluster::put(const std::shared_ptr<const std::string>& key,
                                           const std::shared_ptr<const std::string>& value,
                                           std::shared_ptr<const std::string>& version_out,
                                           KeyType type)
{
  return do_put(key, make_shared<const string>(), value, version_out, type, WriteMode::IGNORE_VERSION);
}

kinetic::KineticStatus KineticCluster::put(const std::shared_ptr<const std::string>& key,
                                           const std::shared_ptr<const std::string>& version,
                                           const std::shared_ptr<const std::string>& value,
                                           std::shared_ptr<const std::string>& version_out,
                                           KeyType type)
{
  return do_put(key, version, value, version_out, type, WriteMode::REQUIRE_SAME_VERSION);
}


void KineticCluster::updateStatistics(std::shared_ptr<DestructionMutex> dm)
{
  std::lock_guard<DestructionMutex> dlock(*dm);

  auto ops = initialize(make_shared<string>("all"), connections.size());
  auto sync = asyncops::fillLog(ops,
                                {Command_GetLog_Type::Command_GetLog_Type_CAPACITIES,
                                 Command_GetLog_Type::Command_GetLog_Type_STATISTICS
                                }
  );
  execute(ops, *sync);

  /* Set up temporary variables */
  uint64_t bytes_total = 0;
  uint64_t bytes_free = 0;
  uint64_t read_ops_total = 0;
  uint64_t read_bytes_total = 0;
  uint64_t write_ops_total = 0;
  uint64_t write_bytes_total = 0;

  /* Evaluate Operation Results. */
  for (auto o = ops.begin(); o != ops.end(); o++) {
    if (!o->callback->getResult().ok()) {
      kio_notice("Could not obtain statistics / capacity information for a drive: ", o->callback->getResult());
      continue;
    }
    const auto& log = std::static_pointer_cast<GetLogCallback>(o->callback)->getLog();

    uint64_t bytes_used = log->capacity.nominal_capacity_in_bytes * log->capacity.portion_full;
    bytes_total += log->capacity.nominal_capacity_in_bytes;
    bytes_free += log->capacity.nominal_capacity_in_bytes - bytes_used;

    for (auto it = log->operation_statistics.cbegin(); it != log->operation_statistics.cend(); it++) {
      if (it->name == "GET_RESPONSE") {
        read_ops_total += it->count;
        read_bytes_total += it->bytes;
      }
      else if (it->name == "PUT") {
        write_ops_total += it->count;
        write_bytes_total += it->bytes;
      }
    }
  }

  /* update cluster variables */
  std::lock_guard<std::mutex> lock(mutex);
  statistics_snapshot.io_start = statistics_snapshot.io_end;
  statistics_snapshot.io_end = std::chrono::system_clock::now();
  statistics_snapshot.read_ops_period = read_ops_total - statistics_snapshot.read_ops_total;
  statistics_snapshot.read_bytes_period = read_bytes_total - statistics_snapshot.read_bytes_total;
  statistics_snapshot.write_ops_period = write_ops_total - statistics_snapshot.write_ops_total;
  statistics_snapshot.write_bytes_period = write_ops_total - statistics_snapshot.write_bytes_total;

  statistics_snapshot.read_ops_total = read_ops_total;
  statistics_snapshot.read_bytes_total = read_bytes_total;
  statistics_snapshot.write_ops_total = write_ops_total;
  statistics_snapshot.write_bytes_total = write_bytes_total;

  statistics_snapshot.bytes_free = bytes_free;
  statistics_snapshot.bytes_total = bytes_total;
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
  kio_debug("Start execution of ", ops.size(), " operations for sync-point ", &sync);
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
          throw std::runtime_error("Connection::Run(...) returned false");
        }
      }
      catch (const std::exception& e) {
        auto status = KineticStatus(StatusCode::CLIENT_IO_ERROR, e.what());
        ops[i].callback->OnResult(status);
        ops[i].connection->setError();
        kio_notice("Failed executing async operation for connection ", ops[i].connection->getName(), status);
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
        } catch (const std::exception& e) {
          kio_warning("Failed removing handle from connection ", ops[i].connection->getName(), "due to: ", e.what());
          ops[i].connection->setError();
        }
        kio_warning("Network timeout for operation ", ops[i].connection->getName(), " for sync-point", &sync);
        auto status = KineticStatus(KineticStatus(StatusCode::CLIENT_IO_ERROR, "Network timeout"));
        ops[i].callback->OnResult(status);
      }

      /* Retry operations with CLIENT_IO_ERROR code result. Something went wrong with the connection,
       * we might just be able to reconnect and make the problem go away. */
      if (rounds_left && ops[i].callback->getResult().statusCode() == StatusCode::CLIENT_IO_ERROR) {
        ops[i].callback->reset();
        need_retry = true;
      }
    }
  } while (need_retry && rounds_left);

  kio_debug("Finished execution for sync-point ", &sync);

  std::map<kinetic::StatusCode, int, KineticCluster::compareStatusCode> map;
  for (auto it = ops.begin(); it != ops.end(); it++) {
    map[it->callback->getResult().statusCode()]++;
  }
  return map;
}

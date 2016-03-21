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
#include <drive_log.h>
#include "Logging.hh"
#include "KineticIoSingleton.hh"

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
  for (size_t off = 0; off < connections.size(); off++) {
    ClusterLogOp log({Command_GetLog_Type::Command_GetLog_Type_LIMITS}, connections, 1, off);
    auto cbs = log.execute(operation_timeout);

    if (cbs.front()->getResult().ok()) {
      const auto& l = cbs.front()->getLog()->limits;

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
    kio().threadpool().run(std::bind(&KineticCluster::updateStatistics, this, dmutex));
    statistics_scheduled = system_clock::now();
    kio_debug("Scheduled statistics update for cluster ", id());
  }
  return statistics_snapshot;
}

KineticStatus KineticCluster::range(const std::shared_ptr<const std::string>& start_key,
                                    const std::shared_ptr<const std::string>& end_key,
                                    std::unique_ptr<std::vector<std::string>>& keys, KeyType type, size_t max_elements)
{
  if (!start_key || !end_key) {
    return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, "invalid input.");
  }

  if (!max_elements) {
    max_elements = cluster_limits[type].max_range_elements;
  }

  ClusterRangeOp rangeop(start_key, end_key, max_elements, connections);

  auto status = rangeop.execute(operation_timeout, redundancy[type]);
  if (status.ok()) {
    rangeop.getKeys(keys);
  }
  kio_debug("Range request from key ", *start_key, " to ", *end_key, " completed with status: ", status);
  return status;
}

KineticStatus KineticCluster::do_remove(const std::shared_ptr<const std::string>& key,
                                        const std::shared_ptr<const std::string>& version,
                                        KeyType type, WriteMode wmode)
{
  if (!key || !version) {
    return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, "invalid input.");
  }

  StripeOperation_DEL delOp(key, version, wmode, connections, redundancy[type]->size());
  try {
    auto status = delOp.execute(operation_timeout, redundancy[type]);
    if (delOp.needsIndicator()) {
      delOp.putIndicatorKey(connections);
    }
    kio_debug("Remove request of key ", *key, " completed with status: ", status);
    return status;
  } catch (std::exception& e) {
    if (wmode == WriteMode::IGNORE_VERSION) {
      kio_error("Irrecoverable error in delete operation for key ", *key, " : ", e.what());
      return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, "");
    }
    if (mayForce(key, type, make_shared<const string>())) {
      return do_remove(key, version, type, WriteMode::IGNORE_VERSION);
    }
    return KineticStatus(StatusCode::REMOTE_VERSION_MISMATCH, "Another client won a concurrent put.");
  }
}

KineticStatus KineticCluster::remove(const std::shared_ptr<const std::string>& key,
                                     const std::shared_ptr<const std::string>& version,
                                     KeyType type)
{
  return do_remove(key, version, type, WriteMode::REQUIRE_SAME_VERSION);
}

kinetic::KineticStatus KineticCluster::remove(const std::shared_ptr<const std::string>& key, KeyType type)
{
  return do_remove(key, make_shared<const string>(), type, WriteMode::IGNORE_VERSION);
}

std::vector<std::shared_ptr<const std::string>> KineticCluster::valueToStripe(const std::string& value, KeyType type)
{
  if (!value.length()) {
    return std::vector<std::shared_ptr<const string>>(redundancy[type]->size(), std::make_shared<const string>());
  }

  std::vector<std::shared_ptr<const string>> stripe;

  auto chunkSize = value.length() < chunkCapacity ? value.length() : chunkCapacity;
  std::shared_ptr<std::string> zero;

  /* Set data chunks of the stripe. If value < stripe size, fill in with 0ed strings. */
  for (size_t i = 0; i < redundancy[type]->numData(); i++) {
    if (i * chunkSize < value.length()) {
      auto chunk = std::make_shared<string>(value.substr(i * chunkSize, chunkSize));
      chunk->resize(chunkSize);
      stripe.push_back(chunk);
    }
    else {
      if (!zero) {
        zero = make_shared<string>();
        zero->resize(chunkSize);
      }
      stripe.push_back(zero);
    }
  }
  /* Set empty strings for parities */
  for (size_t i = 0; i < redundancy[type]->numParity(); i++) {
    stripe.push_back(std::make_shared<const string>());
  }
  /* Compute redundancy */
  redundancy[type]->compute(stripe);

  /* We don't actually want to write the 0ed data chunks used for redundancy computation. So get rid of them. */
  for (size_t index = (value.size() + chunkSize - 1) / chunkSize; index < redundancy[type]->numData(); index++) {
    stripe[index] = std::make_shared<const string>();
  }

  return stripe;
}

/* Handle races, 2 or more clients attempting to put/remove the same key at the same time. */
bool KineticCluster::mayForce(const std::shared_ptr<const std::string>& key, KeyType type,
                              const std::shared_ptr<const std::string>& version, size_t counter)
{
  StripeOperation_GET getVersions(key, true, connections, redundancy[type]->size());
  auto rmap = getVersions.executeOperationVector(operation_timeout);
  auto most_frequent = getVersions.mostFrequentVersion();

  /* Remote not found does not reflect in most_frequent */
  if (version->empty() && rmap[StatusCode::REMOTE_NOT_FOUND] > most_frequent.frequency) {
    most_frequent.version = make_shared<const string>();
    most_frequent.frequency = rmap[StatusCode::REMOTE_NOT_FOUND];
  }

  if (most_frequent.frequency && *version == *most_frequent.version) {
    return true;
  }

  if (most_frequent.frequency >= redundancy[type]->numData()) {
    return false;
  }

  /* Super-Corner-Case: It could be that the client that should win the most_frequent match has crashed. For this
   * reason, all competing clients will wait, polling the key versions. As multiple clients
   * could theoretically be in this loop concurrently, we specify the maximum number of polls by the position of the first
   * occurrence of the supplied version for a chunk. If the situation does not clear up by end of polling period,
   * overwrite permission will be given. */
  if (counter > 10 * getVersions.versionPosition(version)) {
    return true;
  }

  /* 100 ms sleep time */
  usleep(100 * 1000);
  return mayForce(key, type, version, ++counter);
}


kinetic::KineticStatus KineticCluster::execute_get(kio::StripeOperation_GET& getop,
                                                   const std::shared_ptr<const std::string>& key,
                                                   std::shared_ptr<const std::string>& version,
                                                   std::shared_ptr<const std::string>& value, KeyType type)
{
  auto status = getop.execute(operation_timeout, redundancy[type]);

  if (status.ok()) {
    value = getop.getValue();
    version = getop.getVersion();
  }
  if (getop.needsIndicator()) {
    getop.putIndicatorKey(connections);
  }
  return status;
}

kinetic::KineticStatus KineticCluster::do_get(const std::shared_ptr<const std::string>& key,
                                              std::shared_ptr<const std::string>& version,
                                              std::shared_ptr<const std::string>& value, KeyType type, bool skip_value)
{
  if (!key) {
    return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, "invalid input, key has to be supplied.");;
  }
  StripeOperation_GET getop(key, skip_value, connections, redundancy[type]->numData());

  /* Skip attempting to read without parities for replicated keys, version verification is required to validate result */
  if (redundancy[type]->numData() > 1) {
    try {
      return execute_get(getop, key, version, value, type);
    } catch (std::exception& e) {
      kio_debug("Failed getting stripe for key ", *key, " without parities: ", e.what());
    }
  }

  /* Add parity chunks to get request (already obtained chunks will not be re-fetched). */
  getop.extend(connections, redundancy[type]->numParity());
  try {
    return execute_get(getop, key, version, value, type);
  } catch (std::exception& e) {
    kio_debug("Failed getting stripe for key ", *key, " even with parities: ", e.what());
  }

  /* Try to use handoff chunks if any are available to serve the request */
  if (getop.insertHandoffChunks(connections)) {
    try {
      return execute_get(getop, key, version, value, type);
    } catch (std::exception& e) {
      kio_debug("Failed getting stripe for key ", *key, " even with handoff chunks: ", e.what());
    }
  }

  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Key " + *key + " not accessible.");
}

kinetic::KineticStatus KineticCluster::get(const std::shared_ptr<const std::string>& key,
                                           std::shared_ptr<const std::string>& version, KeyType type)
{
  std::shared_ptr<const string> value;
  auto status = do_get(key, version, value, type, true);
  kio_debug("Get VERSION request of key ", *key, " completed with status: ", status);
  return status;
}

kinetic::KineticStatus KineticCluster::get(const std::shared_ptr<const std::string>& key,
                                           std::shared_ptr<const std::string>& version,
                                           std::shared_ptr<const std::string>& value, KeyType type)
{
  auto status = do_get(key, version, value, type, false);
  kio_debug("Get DATA request of key ", *key, " completed with status: ", status);
  return status;
}

KineticStatus KineticCluster::do_put(const std::shared_ptr<const std::string>& key,
                                     const std::shared_ptr<const std::string>& version,
                                     const std::shared_ptr<const std::string>& value,
                                     std::shared_ptr<const std::string>& version_out,
                                     KeyType type,
                                     kinetic::WriteMode mode)
{
  if (!key || !version || !value) {
    return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, "invalid input.");
  }

  /* Do not use version_out variable directly in case the client
     supplies the same pointer for version and version_out. */
  auto version_new = utility::uuidGenerateEncodeSize(value->size());
  std::vector<std::shared_ptr<const string>> stripe;
  try {
    stripe = valueToStripe(*value, type);
  } catch (const std::exception& e) {
    kio_error("Failed building data stripe for key ", *key, ": ", e.what());
    return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, e.what());
  }

  StripeOperation_PUT putOp(key, version_new, version, stripe, mode, connections, redundancy[type]->size());
  try {
    auto status = putOp.execute(operation_timeout, redundancy[type]);
    if (putOp.needsIndicator()) {
      putOp.putIndicatorKey(connections);
      putOp.putHandoffKeys(connections);
    }
    if (status.ok()) {
      version_out = version_new;
    }
    return status;
  } catch (std::exception& e) {
    if (mode == WriteMode::IGNORE_VERSION) {
      kio_error("Irrecoverable error in put operation for key ", *key, " : ", e.what());
      return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, "Invalid.");
    }
    if (mayForce(key, type, version_new)) {
      return do_put(key, version, value, version_out, type, WriteMode::IGNORE_VERSION);
    }
    return KineticStatus(StatusCode::REMOTE_VERSION_MISMATCH, "Another client won the put race.");
  }
}

kinetic::KineticStatus KineticCluster::put(const std::shared_ptr<const std::string>& key,
                                           const std::shared_ptr<const std::string>& value,
                                           std::shared_ptr<const std::string>& version_out,
                                           KeyType type)
{
  auto status = do_put(key, make_shared<const string>(), value, version_out, type, WriteMode::IGNORE_VERSION);
  kio_debug("Forced put request for key ", *key, " completed with status: ", status);
  return status;
}

kinetic::KineticStatus KineticCluster::put(const std::shared_ptr<const std::string>& key,
                                           const std::shared_ptr<const std::string>& version,
                                           const std::shared_ptr<const std::string>& value,
                                           std::shared_ptr<const std::string>& version_out,
                                           KeyType type)
{
  auto status = do_put(key, version ? version : make_shared<const string>(), value, version_out, type,
                       WriteMode::REQUIRE_SAME_VERSION);
  kio_debug("Versioned put request for key ", *key, " completed with status: ", status);
  return status;
}


void KineticCluster::updateStatistics(std::shared_ptr<DestructionMutex> dm)
{
  std::lock_guard<DestructionMutex> dlock(*dm);

  ClusterLogOp logop(
      {
          Command_GetLog_Type::Command_GetLog_Type_CAPACITIES,
          Command_GetLog_Type::Command_GetLog_Type_STATISTICS,
          Command_GetLog_Type::Command_GetLog_Type_UTILIZATIONS
      },
      connections, connections.size()
  );
  auto cbs = logop.execute(operation_timeout);

  /* Set up temporary variables */
  uint64_t bytes_total = 0;
  uint64_t bytes_free = 0;
  uint64_t read_ops_total = 0;
  uint64_t read_bytes_total = 0;
  uint64_t write_ops_total = 0;
  uint64_t write_bytes_total = 0;
  double hda_utilization = 0;
  int failed_connections = 0;

  /* Evaluate Operation Results. */
  for (size_t i = 0; i < cbs.size(); i++) {
    if (!cbs[i]->getResult().ok()) {
      kio_notice("Could not obtain statistics / capacity information for a drive: ", cbs[i]->getResult());
      failed_connections++;
      continue;
    }
    const auto& log = cbs[i]->getLog();

    auto bytes_used = static_cast<size_t>(log->capacity.nominal_capacity_in_bytes * log->capacity.portion_full);
    bytes_total += log->capacity.nominal_capacity_in_bytes;
    bytes_free += log->capacity.nominal_capacity_in_bytes - bytes_used;

    /* extract operation statistics */
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

    /* extract utilization */
    for (auto it = log->utilizations.cbegin(); it != log->utilizations.cend(); it++) {
      if (it->name == "HDA") {
        hda_utilization += it->percent;
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
  statistics_snapshot.write_bytes_period = write_bytes_total - statistics_snapshot.write_bytes_total;

  statistics_snapshot.utilization_hda_period = hda_utilization / (cbs.size() - failed_connections);

  statistics_snapshot.read_ops_total = read_ops_total;
  statistics_snapshot.read_bytes_total = read_bytes_total;
  statistics_snapshot.write_ops_total = write_ops_total;
  statistics_snapshot.write_bytes_total = write_bytes_total;

  statistics_snapshot.bytes_free = bytes_free;
  statistics_snapshot.bytes_total = bytes_total;

}

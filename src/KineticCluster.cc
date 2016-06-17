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

  /* Set initial values for cluster statistics and schedule an update. */
  auto& h = statistics_snapshot.health;
  h.drives_total = static_cast<uint32_t>(connections.size());
  h.redundancy_factor = static_cast<uint32_t>(std::min(rp_data->numParity(), rp_metadata->numParity()));
  statistics_snapshot.bytes_total = 1;
  kio().threadpool().try_run(std::bind(&KineticCluster::updateSnapshot, this, dmutex));
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
    kio().threadpool().run(std::bind(&KineticCluster::updateSnapshot, this, dmutex));
    statistics_scheduled = system_clock::now();
    kio_debug("Scheduled statistics update for cluster ", id());
  }
  return statistics_snapshot;
}

KineticStatus KineticCluster::flush()
{
  ClusterFlushOp flushOp(connections);
  auto status = flushOp.execute(operation_timeout, connections.size() - redundancy.begin()->second->numParity());
  kio_debug("Flush request for cluster ", id(), "completed with status ", status);
  return status;
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

  auto status = rangeop.execute(operation_timeout, connections.size() - redundancy[type]->numParity());
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

  StripeOperation_DEL delOp(key, version, wmode, connections, redundancy[type], redundancy[type]->size());
  auto status = delOp.execute(operation_timeout);
  if (delOp.needsIndicator()) {
    delOp.putIndicatorKey();
  }
  kio_debug("Remove request of key ", *key, " completed with status: ", status);
  return status;
}

KineticStatus KineticCluster::remove(const std::shared_ptr<const std::string>& key, KeyType type)
{
  return do_remove(key, make_shared<const string>(), type, WriteMode::IGNORE_VERSION);
}

KineticStatus KineticCluster::remove(const std::shared_ptr<const std::string>& key,
                                     const std::shared_ptr<const std::string>& version,
                                     KeyType type)
{
  return do_remove(key, version, type, WriteMode::REQUIRE_SAME_VERSION);
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

  /* Compute Stripe */
  std::vector<std::shared_ptr<const string>> stripe;
  try {
    stripe = valueToStripe(*value, type);
  } catch (const std::exception& e) {
    kio_error("Failed building data stripe for key ", *key, ": ", e.what());
    return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, e.what());
  }

  /* Do not use version_out variable directly in case the client uses the same pointer for version and version_out. */
  auto version_new = utility::uuidGenerateEncodeSize(value->size());

  StripeOperation_PUT putOp(key, version_new, version, stripe, mode, connections, redundancy[type]);

  auto status = putOp.execute(operation_timeout);
  if (putOp.needsIndicator()) {
    putOp.putIndicatorKey();
    putOp.putHandoffKeys();
  }
  if (status.ok()) {
    version_out = version_new;
  }
  return status;
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

bool operator==(const StripeOperation_GET::VersionCount& lhs, const StripeOperation_GET::VersionCount& rhs)
{
  if (lhs.frequency != rhs.frequency)
    return false;

  /* special case of both counts showing a 0 frequency */
  if (!lhs.frequency && !rhs.frequency)
    return true;

  if (*lhs.version != *rhs.version)
    return false;
  return true;
}

bool operator!=(const StripeOperation_GET::VersionCount& lhs, const StripeOperation_GET::VersionCount& rhs)
{
  return !(lhs == rhs);
}

kinetic::KineticStatus KineticCluster::do_get(const std::shared_ptr<const std::string>& key,
                                              std::shared_ptr<const std::string>& version,
                                              std::shared_ptr<const std::string>& value, KeyType type, bool skip_value)
{
  if (!key) {
    return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, "invalid input, key has to be supplied.");;
  }
  StripeOperation_GET getop(key, skip_value, connections, redundancy[type]);

  auto status = getop.execute(operation_timeout);

  if (status.statusCode() == StatusCode::CLIENT_IO_ERROR && getop.mostFrequentVersion().frequency) {
    /* If other clients are writing concurrently, we could have read in a mix of chunks. We do not want to return IO
     * error in this case but simply wait until the other client completes and return the valid result at that point.
     * Let's see if there are changes to the stripe version before the configured timeout time. */
    auto start_time = std::chrono::system_clock::now();
    do {
      usleep(200 * 1000);
      StripeOperation_GET getop_concurrency_check(key, skip_value, connections, redundancy[type]);
      getop_concurrency_check.execute(operation_timeout);
      if (getop.mostFrequentVersion() != getop_concurrency_check.mostFrequentVersion()) {
        kio_warning("Concurrent write detected. Re-starting get operation for key ", *key, ".");
        return do_get(key, version, value, type, skip_value);
      }
    } while (std::chrono::system_clock::now() < start_time + operation_timeout);
    kio_warning("No concurrent write: Both pre and post timeout most frequent version is ",
                *getop.mostFrequentVersion().version);
  }

  if (status.ok()) {
    value = getop.getValue();
    version = getop.getVersion();
    kio_debug("status ok for key ", *key, " version is ", *version);
  }
  if (getop.needsIndicator()) {
    getop.putIndicatorKey();
  }
  return status;
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
  if (status.ok())
    kio_debug("Get DATA request of key ", *key, " completed with status: ", status);
  return status;
}


void KineticCluster::updateSnapshot(std::shared_ptr<DestructionMutex> dm)
{
  std::lock_guard<DestructionMutex> dlock(*dm);

  /* Test indicator existence */
  auto indicator_start = utility::makeIndicatorKey(id());
  auto indicator_end = utility::makeIndicatorKey(id() + "~");
  ClusterRangeOp rangeop(indicator_start, indicator_end, 1, connections);
  auto indicator_status = rangeop.execute(operation_timeout,
                                          connections.size() - redundancy[KeyType::Data]->numParity()
  );
  bool indicator = false;
  if (indicator_status.ok()) {
    std::unique_ptr<std::vector<std::string>> keys;
    rangeop.getKeys(keys);
    indicator = keys->size() > 0;
  }

  ClusterLogOp logop({Command_GetLog_Type::Command_GetLog_Type_CAPACITIES,
                      Command_GetLog_Type::Command_GetLog_Type_STATISTICS
                     }, connections, connections.size());
  auto cbs = logop.execute(operation_timeout);

  /* Set up temporary variables */
  uint32_t num_failed = 0;

  uint64_t bytes_total = 0;
  uint64_t bytes_free = 0;

  uint64_t read_ops_total = 0;
  uint64_t read_bytes_total = 0;
  uint64_t write_ops_total = 0;
  uint64_t write_bytes_total = 0;

  /* Evaluate Log Operation Results. */
  for (size_t i = 0; i < cbs.size(); i++) {
    if (!cbs[i]->getResult().ok()) {
      kio_notice("Could not obtain statistics / capacity information for a drive: ", cbs[i]->getResult());
      num_failed++;
      continue;
    }

    const auto& log = cbs[i]->getLog();

    auto bytes_used = static_cast<uint64_t>(log->capacity.nominal_capacity_in_bytes * log->capacity.portion_full);
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
  statistics_snapshot.write_bytes_period = write_bytes_total - statistics_snapshot.write_bytes_total;

  statistics_snapshot.read_ops_total = read_ops_total;
  statistics_snapshot.read_bytes_total = read_bytes_total;
  statistics_snapshot.write_ops_total = write_ops_total;
  statistics_snapshot.write_bytes_total = write_bytes_total;

  statistics_snapshot.bytes_free = bytes_free;
  statistics_snapshot.bytes_total = bytes_total;

  statistics_snapshot.health.indicator_exist = indicator;
  statistics_snapshot.health.drives_failed = num_failed;
}
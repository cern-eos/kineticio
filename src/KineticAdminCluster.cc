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

#include "KineticAdminCluster.hh"
#include <Logging.hh>

using namespace kio;
using namespace kinetic;
using std::string;

KineticAdminCluster::~KineticAdminCluster()
{
}

ClusterStatus KineticAdminCluster::status()
{
  ClusterStatus clusterStatus;

  clusterStatus.redundancy_factor = redundancy[KeyType::Data]->numParity();
  clusterStatus.drives_total = redundancy[KeyType::Data]->size();
  clusterStatus.drives_failed = 0;

  /* Set indicator existance */
  std::shared_ptr<const string> indicator_start;
  std::shared_ptr<const string> indicator_end;
  initRangeKeys(OperationTarget::INDICATOR, indicator_start, indicator_end);

  std::unique_ptr<std::vector<string>> keys(new std::vector<string>());
  auto status = range(indicator_start, indicator_end, keys, KeyType::Data, 1);

  clusterStatus.indicator_exist = keys->size() > 0;

  /* Set individual connection info */
  for (auto it = connections.cbegin(); it != connections.cend(); it++) {
    auto& con = *it;
    clusterStatus.location.push_back(con->getName());
    try {
      con->get();
      clusterStatus.connected.push_back(true);
    } catch (std::exception& e) {
      clusterStatus.connected.push_back(false);
      clusterStatus.drives_failed++;
    }
  }

  return clusterStatus;
}

bool KineticAdminCluster::removeIndicatorKey(const std::shared_ptr<const string>& key)
{
  StripeOperation_DEL rm(key, std::make_shared<const string>(), WriteMode::IGNORE_VERSION, connections, connections.size());
  auto status = rm.execute(operation_timeout, redundancy[KeyType::Data]);
  return status.ok();
}

bool KineticAdminCluster::scanKey(const std::shared_ptr<const string>& key, KeyType keyType,
                                  KeyCountsInternal& key_counts)
{
  StripeOperation_GET getV(key, true, connections, redundancy[keyType]->size());
  auto rmap = getV.executeOperationVector(operation_timeout);
  auto valid_results = rmap[StatusCode::OK] + rmap[StatusCode::REMOTE_NOT_FOUND];
  auto target_version = getV.mostFrequentVersion();

  auto debugstring = kio::utility::Convert::toString(
      valid_results, " of ", redundancy[keyType]->size(), " drives returned a result. Key is available on ",
      rmap[StatusCode::OK], " drives. ", target_version.frequency, " drives have an equivalent version (",
      redundancy[keyType]->numData(), ") needed."
  );

  if (valid_results < redundancy[keyType]->size()) {
    kio_notice("Key \"", *key, "\" is incomplete. Only ", valid_results, " of ", redundancy[keyType]->size(),
              " drives returned a result");
    key_counts.incomplete++;
  }
  if (target_version.frequency == valid_results && valid_results >= redundancy[keyType]->numData()) {
    kio_debug("Key \"", *key, "\" does not require action. ", debugstring);
    return false;
  }
  else if (target_version.frequency >= redundancy[keyType]->numData() ||
           rmap[StatusCode::REMOTE_NOT_FOUND] >= redundancy[keyType]->numData()) {
    kio_notice("Key \"", *key, "\" requires repair or removal. ", debugstring);
    key_counts.need_action++;
    return true;
  }

  kio_error("Key ", *key, " is unfixable. ", debugstring);

  throw std::runtime_error("unfixable");
}

void KineticAdminCluster::repairKey(const std::shared_ptr<const string>& key, KeyType keyType,
                                    KeyCountsInternal& key_counts)
{
  std::shared_ptr<const string> version;
  std::shared_ptr<const string> value;

  auto getstatus = this->get(key, version, value, keyType);
  if (getstatus.ok()) {
    auto putstatus = this->put(key, version, value, version, keyType);
    if (!putstatus.ok()) {
      kio_warning("Failed put operation on target-key \"", *key, "\" ", putstatus);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    }
    key_counts.repaired++;
  }

  else if (getstatus.statusCode() == StatusCode::REMOTE_NOT_FOUND) {
    auto rmstatus = this->remove(key, keyType);
    if (!rmstatus.ok()) {
      kio_warning("Failed remove operation on target-key \"", *key, "\" ", rmstatus);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    }
    key_counts.removed++;
  }

  else {
    kio_warning("Failed get operation on target-key \"", *key, "\" ", getstatus);
    throw std::system_error(std::make_error_code(std::errc::io_error));
  };
}

bool isDataKey(const string& clusterId, const string& key)
{
  std::string prefix = clusterId + ":data:";
  return key.compare(0, prefix.length(), prefix) == 0;
}

void KineticAdminCluster::applyOperation(
    Operation operation, OperationTarget target, KeyCountsInternal& key_counts,
    std::vector<std::shared_ptr<const string>> keys
)
{
  auto keyType = target == OperationTarget::DATA ? KeyType::Data : KeyType::Metadata;

  for (auto it = keys.begin(); it != keys.end(); it++) {
    std::shared_ptr<const string> key = *it;

    /* Special consideration applies when traversing indicator keys... as they can indicate both data and
     * metadata keys */
    if (target == OperationTarget::INDICATOR) {
      key = utility::indicatorToKey(*key);
      if (isDataKey(id(), *key)) {
        keyType = KeyType::Data;
        kio_debug("Indicator key ", **it, " points to DATA key ", *key);
      }
      else {
        keyType = KeyType::Metadata;
        kio_debug("Indicator key ", **it, " points to NON-DATA key ", *key);
      }
    }

    try {
      switch (operation) {
        case Operation::SCAN: {
          scanKey(key, keyType, key_counts);
          break;
        }
        case Operation::REPAIR: {
          if (scanKey(key, keyType, key_counts) || target == OperationTarget::INDICATOR) {
            repairKey(key, keyType, key_counts);
          }
          if (target == OperationTarget::INDICATOR) {
            removeIndicatorKey(*it);
          }
          break;
        }
        case Operation::RESET: {
          if (target == OperationTarget::INDICATOR) {
            if (!removeIndicatorKey(*it)) {
              throw std::system_error(std::make_error_code(std::errc::io_error));
            }
          }
          else {
            auto rmstatus = remove(key, KeyType::Data);
            if (!rmstatus.ok()) {
              kio_warning("Failed remove operation on target-key \"", *key, "\" ", rmstatus);
              throw std::system_error(std::make_error_code(std::errc::io_error));
            };
          }
          key_counts.removed++;
          break;
        }
      }
    } catch (const std::exception& e) {
      key_counts.unrepairable++;
    }
  }
}

void KineticAdminCluster::initRangeKeys(
    OperationTarget t,
    std::shared_ptr<const string>& start_key,
    std::shared_ptr<const string>& end_key
)
{
  auto id = this->id();
  switch (t) {
    case OperationTarget::METADATA:
      start_key = utility::makeMetadataKey(id, " ");
      end_key = utility::makeMetadataKey(id, "~");
      break;
    case OperationTarget::ATTRIBUTE:
      start_key = utility::makeAttributeKey(id, " ", " ");
      end_key = utility::makeAttributeKey(id, "~", "~");
      break;
    case OperationTarget::DATA:
      start_key = utility::makeDataKey(id, " ", 0);
      end_key = utility::makeDataKey(id, "~", 99999999);
      break;
    case OperationTarget::INDICATOR:
      start_key = utility::makeIndicatorKey(id);
      end_key = utility::makeIndicatorKey(id + "~");
      break;
  }
  kio_debug("Start key=", *start_key);
  kio_debug("End key=", *end_key);
}

kio::AdminClusterInterface::KeyCounts KineticAdminCluster::doOperation(
    Operation o,
    OperationTarget t,
    std::function<void(int)> callback,
    int numthreads
)
{
  KeyCountsInternal key_counts;

  std::shared_ptr<const string> start_key;
  std::shared_ptr<const string> end_key;
  initRangeKeys(t, start_key, end_key);

  {
    BackgroundOperationHandler bg(numthreads, numthreads);
    std::unique_ptr<std::vector<string>> keys;
    do {
      /* KeyType for range requests does not matter at the moment */
      auto status = range(start_key, end_key, keys, KeyType::Data);
      if (!status.ok()) {
        kio_warning("range(", *start_key, " - ", *end_key, ") failed on cluster. Cannot proceed. ", status);
        break;
      }
      if (keys && keys->size()) {
        start_key = std::make_shared<const string>(keys->back() + static_cast<char>(0));
        key_counts.total += keys->size();

        if (o != Operation::COUNT) {
          std::vector<std::shared_ptr<const string>> out;
          for (auto it = keys->cbegin(); it != keys->cend(); it++) {
            out.push_back(std::make_shared<const string>(std::move(*it)));
          }
          bg.run(std::bind(&KineticAdminCluster::applyOperation, this, o, t, std::ref(key_counts), out));
        }
      }
      if (callback) {
        callback(key_counts.total);
      }
    } while (keys && keys->size());
  }

  return KeyCounts{key_counts.total, key_counts.incomplete, key_counts.need_action,
                   key_counts.repaired, key_counts.removed, key_counts.unrepairable
  };
}

int KineticAdminCluster::count(OperationTarget target, std::function<void(int)> callback)
{
  return doOperation(Operation::COUNT, target, std::move(callback), 0).total;
}

kio::AdminClusterInterface::KeyCounts KineticAdminCluster::scan(OperationTarget target,
                                                                std::function<void(int)> callback, int numThreads)
{
  return doOperation(Operation::SCAN, target, std::move(callback), numThreads);
}

kio::AdminClusterInterface::KeyCounts KineticAdminCluster::repair(OperationTarget target,
                                                                  std::function<void(int)> callback, int numThreads)
{
  return doOperation(Operation::REPAIR, target, std::move(callback), numThreads);
}

kio::AdminClusterInterface::KeyCounts KineticAdminCluster::reset(OperationTarget target,
                                                                 std::function<void(int)> callback, int numThreads)
{
  return doOperation(Operation::RESET, target, std::move(callback), numThreads);
}
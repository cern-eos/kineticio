#include "KineticAdminCluster.hh"
#include <Logging.hh>

using namespace kio;
using namespace kinetic;
using std::string;

KineticAdminCluster::~KineticAdminCluster()
{
}

std::vector<std::pair<bool, std::string>> KineticAdminCluster::status()
{
  std::vector<std::pair<bool, std::string>> statusVector;

  for (auto it = connections.cbegin(); it != connections.cend(); it++) {
    auto& con = *it;
    statusVector.push_back(std::make_pair(true, con->getName()));
    try {
      con->get();
    } catch (std::exception& e) {
      statusVector.back().first = false;
    }
  }
  return statusVector;
}

bool isIndicatorKey(const string& key)
{
  auto empty_indicator = utility::makeIndicatorKey("");
  return key.compare(0, empty_indicator->length(), *empty_indicator) == 0;
}

bool KineticAdminCluster::scanKey(const std::shared_ptr<const string>& key, KeyCountsInternal& key_counts)
{
  auto ops = initialize(key, nData + nParity);
  auto sync = asyncops::fillGetVersion(ops, key);
  auto rmap = execute(ops, *sync);

  auto valid_results = rmap[StatusCode::OK] + rmap[StatusCode::REMOTE_NOT_FOUND];
  auto target_version = asyncops::mostFrequentVersion(ops);

  if (valid_results < nData + nParity) {
    kio_debug("Key \"", *key, "\": Only ", valid_results, " valid results for a stripe size of ", nData + nParity);
    key_counts.incomplete++;
  }
  if (target_version.frequency == valid_results && valid_results >= nData) {
    kio_debug("Key \"", *key, "\" does not require action.");
    return false;
  }
  else if (target_version.frequency >= nData || rmap[StatusCode::REMOTE_NOT_FOUND] >= nData) {
    kio_debug("Key \"", *key, "\" requires repair or removal.");
    key_counts.need_action++;
    return true;
  }

  throw std::runtime_error(utility::Convert::toString(
      "Key ", *key, " is unfixable. We succeeded to read from ", valid_results, " of ", nData + nParity, " drives. ",
      rmap[StatusCode::REMOTE_NOT_FOUND], " drives do not store the key. ",
      rmap[StatusCode::OK], " drives store they key, ", target_version.frequency,
      " of which have an equivalent version "
          "(", nData, ") needed."
  ));
}

void KineticAdminCluster::repairKey(const std::shared_ptr<const string>& key, KeyCountsInternal& key_counts)
{
  std::shared_ptr<const string> version;
  std::shared_ptr<const string> value;

  auto getstatus = this->get(key, false, version, value);
  if (getstatus.ok()) {
    auto putstatus = this->put(key, version, value, false, version);
    if (!putstatus.ok()) {
      kio_warning("Failed put operation on target-key \"", *key, "\" ", putstatus);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    }
    key_counts.repaired++;
  }

  else if (getstatus.statusCode() == StatusCode::REMOTE_NOT_FOUND) {
    auto rmstatus = this->remove(key, version, true);
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

void KineticAdminCluster::applyOperation(Operation o, KeyCountsInternal& key_counts,
                                         std::vector<std::shared_ptr<const string>> keys)
{
  for (auto it = keys.cbegin(); it != keys.cend(); it++) {
    auto& key = isIndicatorKey(**it) ? utility::indicatorToKey(**it) : *it;
    try {
      switch (o) {
        case Operation::SCAN: {
          scanKey(key, key_counts);
          break;
        }
        case Operation::REPAIR: {
          if (scanKey(key, key_counts) || isIndicatorKey(**it)) {
            repairKey(key, key_counts);
          }

          if (isIndicatorKey(**it)) {
            auto istatus = remove(*it, std::make_shared<const string>(), true);
            if (!istatus.ok())
              kio_warning("Failed removing indicator key after repair for target-key \"", *key, "\" ", istatus);
          }
          break;
        }
        case Operation::RESET: {
          auto rmstatus = this->remove(*it, std::make_shared<const string>(), true);
          if (!rmstatus.ok()) {
            kio_warning("Failed remove operation on target-key \"", **it, "\" ", rmstatus);
            throw std::system_error(std::make_error_code(std::errc::io_error));
          };
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
      auto status = range(start_key, end_key, 100, keys);
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
          bg.run(std::bind(&KineticAdminCluster::applyOperation, this, o, std::ref(key_counts), out));
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
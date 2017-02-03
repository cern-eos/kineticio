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
#include <algorithm>
#include <zconf.h>
#include <iomanip>

using namespace kio;
using namespace kinetic;
using std::string;

KineticAdminCluster::~KineticAdminCluster()
{
}

namespace {
/* Functions intended to benchmark individual connections */
int finish_timed_operation(
    std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection>& con,
    std::chrono::system_clock::time_point& run_start,
    std::shared_ptr<CallbackSynchronization>& sync,
    std::shared_ptr<kio::KineticCallback> cb
)
{
  fd_set x;  int y;
  con->Run(&x, &x, &y);
  std::chrono::system_clock::time_point timeout_time = run_start + std::chrono::seconds(10);
  sync->wait_until(timeout_time);
  auto run_end = std::chrono::system_clock::now();

  if (!cb->getResult().ok()) {
    kio_debug(cb->getResult());
    throw std::runtime_error("Failed timed operation.");
  }
  return (int) std::chrono::duration_cast<std::chrono::milliseconds>(run_end - run_start).count();
};

int timed_put(
    std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection>& con,
    std::string key
)
{
  auto sync = std::make_shared<CallbackSynchronization>();
  auto cb = std::make_shared<kio::PutCallback>(sync);
  std::string value(1024*1024, 'x');
  auto record = std::make_shared<KineticRecord>(
      value, "", "", com::seagate::kinetic::client::proto::Command_Algorithm_INVALID_ALGORITHM
  );

  auto run_start = std::chrono::system_clock::now();
  con->Put(key, "", WriteMode::IGNORE_VERSION, record, cb);
  return finish_timed_operation(con,run_start,sync,cb);
}

int timed_remove(
    std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection>& con,
    std::string key
)
{
  auto sync = std::make_shared<CallbackSynchronization>();
  auto cb = std::make_shared<kio::BasicCallback>(sync);

  auto run_start = std::chrono::system_clock::now();
  con->Delete(key, "", WriteMode::IGNORE_VERSION, cb);
  return finish_timed_operation(con,run_start,sync,cb);
}

int timed_get(
    std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection>& con,
    std::string key
)
{
  auto sync = std::make_shared<CallbackSynchronization>();
  auto cb = std::make_shared<kio::GetCallback>(sync);
  auto run_start = std::chrono::system_clock::now();
  con->Get(key,cb);
  return finish_timed_operation(con,run_start,sync,cb);
}

float benchmark_function(
    std::vector<std::string> keys,
    std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection>& connection,
    std::function<int(std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection>&, std::string)> fun)
{
  int timing = 1;
  for (auto key = keys.begin(); key != keys.end(); key++){
    timing += fun(connection, *key);
  }
  return keys.size() / (timing / 1000.0);
}

}

ClusterStatus KineticAdminCluster::status(int num_bench_keys)
{
  /* Set individual connection info */
  std::vector<std::string> location;
  std::vector<bool> connected;

  for (auto it = connections.cbegin(); it != connections.cend(); it++) {
    auto& con = *it;
    location.push_back(con->getName());
    try {
      auto async_con = con->get();

      if(num_bench_keys) {
        /* create benchmark keys */
        std::vector<std::string> keys;
        for (int i = 0; i < num_bench_keys; i++) {
          std::stringstream ss;
          ss << std::setw(10) << std::setfill('0') << i;
          keys.push_back("benchmark_key_" + ss.str());
        }
        auto seq_put = benchmark_function(keys, async_con, timed_put);
        auto seq_get = benchmark_function(keys, async_con, timed_get);
        benchmark_function(keys, async_con, timed_remove);

        std::random_shuffle(keys.begin(), keys.end());
        auto rnd_put = benchmark_function(keys, async_con, timed_put);
        std::random_shuffle(keys.begin(), keys.end());
        auto rnd_get = benchmark_function(keys, async_con, timed_get);
        std::random_shuffle(keys.begin(), keys.end());
        benchmark_function(keys, async_con, timed_remove);

        location.back() += utility::Convert::toString(
            std::fixed, std::setprecision(1),
            " :: MB/s seq_put=", seq_put, " seq_get=", seq_get, " rnd_put=", rnd_put, " rnd_get=", rnd_get
        );
      }
      connected.push_back(true);
    } catch (std::exception& e) {
      kio_debug(e.what());
      connected.push_back(false);
    }
  }

  while(stats().bytes_total == 1){
    usleep(100);
  }

  auto clusterStatus = stats().health;
  clusterStatus.connected = connected;
  clusterStatus.location = location;
  return clusterStatus;
}

bool KineticAdminCluster::removeIndicatorKey(const std::shared_ptr<const string>& key)
{
  /* Remove any existing handoff keys */
  ClusterRangeOp hofs(std::make_shared<const std::string>("handoff=" + *key),
                      std::make_shared<const std::string>("handoff=" + *key + "~"),
                      100, connections);
  auto status = hofs.execute(operation_timeout, connections.size() - redundancy->numData());

  if (status.ok()) {
    std::unique_ptr<std::vector<std::string>> keys;
    hofs.getKeys(keys);

    for (size_t i = 0; i < keys->size(); i++) {
      StripeOperation_DEL rm(std::make_shared<const std::string>(keys->at(i)),
                             std::make_shared<const string>(),
                             WriteMode::IGNORE_VERSION,
                             connections, redundancy, connections.size());
      rm.execute(operation_timeout);
    }
  }

  /* Remove indicator key */
  StripeOperation_DEL rm(utility::makeIndicatorKey(*key), std::make_shared<const string>(), WriteMode::IGNORE_VERSION,
                         connections, redundancy, connections.size());
  status = rm.execute(operation_timeout);
  return status.ok();
}

bool KineticAdminCluster::scanKey(const std::shared_ptr<const string>& key, KeyCountsInternal& key_counts)
{
  StripeOperation_GET getV(key, true, connections, redundancy, true);
  auto rmap = getV.executeOperationVector(operation_timeout);
  auto valid_results = rmap[StatusCode::OK] + rmap[StatusCode::REMOTE_NOT_FOUND];
  auto target_version = getV.mostFrequentVersion();

  auto debugstring = kio::utility::Convert::toString(
      valid_results, " of ", redundancy->size(), " drives returned a result. Key is available on ",
      rmap[StatusCode::OK], " drives. ", target_version.frequency, " drives have an equivalent version (",
      redundancy->numData(), ") needed."
  );

  if (valid_results < redundancy->size()) {
    kio_notice("Key \"", *key, "\" is incomplete. Only ", valid_results, " of ", redundancy->size(),
               " drives returned a result");
    key_counts.incomplete++;
  }
  if (target_version.frequency == valid_results && valid_results >= redundancy->numData()) {
    kio_debug("Key \"", *key, "\" does not require action. ", debugstring);
    return false;
  }
  else if (target_version.frequency >= redundancy->numData() ||
           rmap[StatusCode::REMOTE_NOT_FOUND] >= redundancy->numData()) {
    kio_notice("Key \"", *key, "\" requires repair or removal. ", debugstring);
    key_counts.need_action++;
    return true;
  }

  kio_error("Key ", *key, " is unfixable. ", debugstring);
  throw std::runtime_error("unfixable");
}

void KineticAdminCluster::repairKey(const std::shared_ptr<const string>& key, KeyCountsInternal& key_counts)
{
  StripeOperation_GET getOperation(key, false, connections, redundancy, true);
  auto getStatus = getOperation.execute(operation_timeout);
    
  if(getStatus.ok()) { 
    auto value = getOperation.getValue(); 
    auto version = getOperation.getVersion();
    auto stripe = this->valueToStripe(*value);
    
    StripeOperation_PUT putOperation(key, version, version, stripe, kinetic::WriteMode::REQUIRE_SAME_VERSION, connections, redundancy);
    if(!putOperation.quick_repair(operation_timeout, getOperation)) {
        auto putstatus = this->put(key, version, value, version);
        if (!putstatus.ok()) {
          kio_warning("Failed put operation on target-key \"", *key, "\" ", putstatus);
          throw std::system_error(std::make_error_code(std::errc::io_error));
        }      
    }
    key_counts.repaired++;
  }
  
  else if (getStatus.statusCode() == StatusCode::REMOTE_NOT_FOUND) {
    auto rmstatus = this->remove(key);
    if (!rmstatus.ok() && rmstatus.statusCode() != StatusCode::REMOTE_NOT_FOUND) {
      kio_warning("Failed remove operation on target-key \"", *key, "\" ", rmstatus);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    }
    key_counts.removed++;
  }
  
  else {
    kio_warning("Failed get operation on target-key \"", *key, "\" ", getStatus);
    throw std::system_error(std::make_error_code(std::errc::io_error));
  };
}

void KineticAdminCluster::applyOperation(
    Operation operation, OperationTarget target, KeyCountsInternal& key_counts,
    std::vector<std::shared_ptr<const string>> keys
)
{
  for (auto it = keys.cbegin(); it != keys.cend(); it++) {
    std::shared_ptr<const string> key = *it;

    if (target == OperationTarget::INDICATOR) {
      key = utility::indicatorToKey(*key);
    }

    try {
      switch (operation) {
        case Operation::SCAN: {
          scanKey(key, key_counts);
          break;
        }
        case Operation::REPAIR: {
          if (scanKey(key, key_counts) || target == OperationTarget::INDICATOR) {
            repairKey(key, key_counts);
            removeIndicatorKey(key);
          }
          break;
        }
        case Operation::RESET: {
          if (target == OperationTarget::INDICATOR) {
            if (!removeIndicatorKey(key)) {
              throw std::system_error(std::make_error_code(std::errc::io_error));
            }
          }
          else {
            auto rmstatus = remove(key);
            if (!rmstatus.ok()) {
              kio_warning("Failed remove operation on target-key \"", *key, "\" ", rmstatus);
              throw std::system_error(std::make_error_code(std::errc::io_error));
            };
          }
          key_counts.removed++;
          break;
        }
        default:
          throw std::runtime_error("Invalid Operation");
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
    case OperationTarget::INVALID:
      throw std::logic_error("Invalid Operation Target Type");
  }
  kio_debug("Start key=", *start_key);
  kio_debug("End key=", *end_key);
}

kio::AdminClusterInterface::KeyCounts KineticAdminCluster::doOperation(
    Operation o,
    OperationTarget t,
    callback_t callback,
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
      auto status = range(start_key, end_key, keys);
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
      if (callback && !callback(key_counts.total)) {
        kio_notice("Callback result indicates shutdown request... interrupting execution.");
        break;
      }
    } while (keys && keys->size());
  }

  return KeyCounts{key_counts.total, key_counts.incomplete, key_counts.need_action,
                   key_counts.repaired, key_counts.removed, key_counts.unrepairable
  };
}

int KineticAdminCluster::count(OperationTarget target, callback_t callback)
{
  return doOperation(Operation::COUNT, target, std::move(callback), 0).total;
}

kio::AdminClusterInterface::KeyCounts KineticAdminCluster::scan(OperationTarget target,
                                                                callback_t callback, int numThreads)
{
  return doOperation(Operation::SCAN, target, std::move(callback), numThreads);
}

kio::AdminClusterInterface::KeyCounts KineticAdminCluster::repair(OperationTarget target,
                                                                  callback_t callback, int numThreads)
{
  return doOperation(Operation::REPAIR, target, std::move(callback), numThreads);
}

kio::AdminClusterInterface::KeyCounts KineticAdminCluster::reset(OperationTarget target,
                                                                 callback_t callback, int numThreads)
{
  return doOperation(Operation::RESET, target, std::move(callback), numThreads);
}
#include "KineticAdminCluster.hh"
#include <Logging.hh>

using namespace kio;
using namespace kinetic;
using std::string;

KineticAdminCluster::~KineticAdminCluster()
{
}

std::vector<bool> KineticAdminCluster::status()
{
  std::vector<bool> sv(connections.size(), true);

  for(int i=0; i<connections.size(); i++){
    try{
      connections[i]->get();
    }catch(std::exception &e){
      sv[i] = false;
    }
  }
  return sv;
}

bool KineticAdminCluster::scanKey(const std::shared_ptr<const string>& key, KeyCountsInternal& key_counts)
{
  auto ops = initialize(key, nData + nParity);
  auto sync = asyncops::fillGetVersion(ops, key);
  auto rmap = execute(ops, *sync);

  auto valid_results = rmap[StatusCode::OK] + rmap[StatusCode::REMOTE_NOT_FOUND];
  auto target_version = asyncops::mostFrequentVersion(ops);

  if(valid_results < nData+nParity){
    kio_debug("Not all drives for key \"", *key, "\" could be accessed: Only ", valid_results, " valid results for a "
        "stripe size of ", nData+nParity);
    key_counts.incomplete++;
  }
  if(target_version.frequency == valid_results && valid_results >= nData) {
    kio_debug("Key \"", *key, "\" does not require repair.");
    return false;
  }
  else if(target_version.frequency >=  nData || rmap[StatusCode::REMOTE_NOT_FOUND] >= nData) {
    kio_debug("Key \"", *key, "\" requires repair or removal.");
    key_counts.need_action++;
    return true;
  }

  throw std::runtime_error(utility::Convert::toString(
      "Key ", *key, " is unfixable. We succeeded to read from ", valid_results, " of ", nData+nParity, " drives. ",
      rmap[StatusCode::REMOTE_NOT_FOUND], " drives do not store the key. ",
      rmap[StatusCode::OK], " drives store they key, ", target_version.frequency, " of which have an equivalent version "
      "(", nData, ") needed."
  ));
}

bool isIndicatorKey(const string& key){
  auto empty_indicator = utility::keyToIndicator("");
  return key.compare(0, empty_indicator->length(), *empty_indicator) == 0;
}

void KineticAdminCluster::applyOperation(
    Operation o, OperationTarget t, KeyCountsInternal& key_counts,
    std::vector<std::shared_ptr<const string>> keys)
{
  auto version = std::make_shared<const string>();
  auto value = std::make_shared<const string>();

  for(auto it = keys.cbegin(); it != keys.cend(); it++) {
    /* If we are traversing all keys anyways, we can skip indicator keys unless we are in a reset operation. */
    if(t==OperationTarget::ALL && o != Operation::RESET && isIndicatorKey(**it))
      continue; 
    
    auto& key = t ==OperationTarget::INDICATOR ? utility::indicatorToKey(**it) : *it;
    try {
      switch(o){
        /* Nothing to do but scan. */
        case Operation::SCAN:
          scanKey(key,key_counts);
          break;
          
        /* In case of RESET, we want to target the actual indicator keys if they are supplied... so using *it here. */
        case Operation::RESET: {
          auto rmstatus = this->remove(*it, version, true);
          if (!rmstatus.ok())
           throw kio_exception(EIO, "Failed remove operation on target-key \"", **it, "\" ", rmstatus);
          key_counts.removed++;
          break;
        }
          
        /* Repair key only if the scan tells us to. If the operation is performed using indicator keys, 
         * we can remove the indicator after the repair operation. */
        case Operation::REPAIR: {
          if(scanKey(key, key_counts)){
            auto getstatus = this->get(key, false, version, value);
            if (getstatus.ok()) {
                auto putstatus = this->put(key, version, value, false, version);
                if (!putstatus.ok())
                  throw kio_exception(EIO, "Failed put operation on target-key \"", *key, "\" ", putstatus);
                key_counts.repaired++;
            }
            else if (getstatus.statusCode() == StatusCode::REMOTE_NOT_FOUND) {
              auto rmstatus = this->remove(key, version, true);
              if (!rmstatus.ok())
                throw kio_exception(EIO, "Failed remove operation on target-key \"", *key, "\" ", rmstatus);
              key_counts.removed++;
            }
            else
              throw kio_exception(EIO, "Failed get operation on target-key \"", *key, "\" ", getstatus);
          }
          if(t == OperationTarget::INDICATOR || t == OperationTarget::ALL){
            auto istatus = remove(*it, std::make_shared<const string>(), true);
            if(!istatus.ok() && t == OperationTarget::INDICATOR)
              kio_warning("Failed removing indicator key after repair for target-key \"", *key, "\" ", istatus);
          }
          break;
        }
      }     
    } catch (const std::exception& e) {
      key_counts.unrepairable++;
      kio_warning(e.what());
    }
  }
}

void initRangeKeys(
    kio::KineticAdminCluster::OperationTarget t, 
    std::shared_ptr<const string>& start_key, 
    std::shared_ptr<const string>& end_key
)
{
  end_key = std::make_shared<const string>(1, static_cast<char>(255));
  switch(t){
    case kio::KineticAdminCluster::OperationTarget::ALL: 
      start_key = std::make_shared<const string>(1, static_cast<char>(0));
      break;
    case kio::KineticAdminCluster::OperationTarget::ATTRIBUTE:
      start_key = utility::constructAttributeKey("","");
      end_key = utility::constructAttributeKey(*end_key, *end_key);
      break;
    case kio::KineticAdminCluster::OperationTarget::FILE:
      start_key = std::make_shared<const string>("/");
      break;
    case kio::KineticAdminCluster::OperationTarget::INDICATOR:
      start_key = utility::keyToIndicator("");
      end_key = utility::keyToIndicator(*end_key);
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
    BackgroundOperationHandler bg(numthreads,numthreads); 
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
          for (auto it = keys->cbegin(); it != keys->cend(); it++){
             out.push_back(std::make_shared<const string>(std::move(*it)));
          }
          bg.run(std::bind(&KineticAdminCluster::applyOperation, this, o, t, std::ref(key_counts), out));
        }
        if(callback)
          callback(key_counts.total);
      }
    } while (keys && keys->size());
  }
  
  return KeyCounts{key_counts.total, key_counts.incomplete, key_counts.need_action, 
                   key_counts.repaired, key_counts.removed, key_counts.unrepairable}; 
}

int KineticAdminCluster::count(OperationTarget target, std::function<void(int)> callback)
{
  return doOperation(Operation::COUNT, target, std::move(callback), 0).total;
}

kio::AdminClusterInterface::KeyCounts KineticAdminCluster::scan(OperationTarget target, std::function<void(int)> callback, int numThreads)
{
  return doOperation(Operation::SCAN, target, std::move(callback), numThreads);
}

kio::AdminClusterInterface::KeyCounts KineticAdminCluster::repair(OperationTarget target, std::function<void(int)> callback, int numThreads)
{
  return doOperation(Operation::REPAIR, target, std::move(callback), numThreads);
}

kio::AdminClusterInterface::KeyCounts KineticAdminCluster::reset(OperationTarget target, std::function<void(int)> callback, int numThreads)
{
  return doOperation(Operation::RESET, target, std::move(callback), numThreads);
}
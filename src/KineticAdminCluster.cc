#include "KineticAdminCluster.hh"
#include <Logging.hh>

using namespace kio;
using namespace kinetic;

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

bool KineticAdminCluster::scanKey(const std::shared_ptr<const std::string>& key)
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
    key_counts.need_repair++;
    return true;
  }

  throw std::runtime_error(utility::Convert::toString(
      "Key ", *key, " is unfixable. We succeeded to read from ", valid_results, " of ", nData+nParity, " drives. ",
      rmap[StatusCode::REMOTE_NOT_FOUND], " drives do not store the key. ",
      rmap[StatusCode::OK], " drives store they key, ", target_version.frequency, " of which have an equivalent version "
      "(", nData, ") needed."
  ));
}

void KineticAdminCluster::applyOperation(
    Operation o,
    std::vector<std::shared_ptr<const std::string>> keys)
{
  auto version = std::make_shared<const string>("");
  auto value = std::make_shared<const string>("");

  for(auto it = keys.cbegin(); it != keys.cend(); it++) {
    try {
      auto need_action = scanKey(*it);
      if(o == Operation::SCAN)
        continue; 
      
      /* set default to NOT_FOUND so remove will be called for o == RESET */
      KineticStatus getstatus(StatusCode::REMOTE_NOT_FOUND, "");
      if(need_action && o == Operation::REPAIR)
        getstatus = this->get(*it, false, version, value);
      
      /* At this point we can remove the potentially existing indicator key. */
      auto istatus = remove(utility::keyToIndicator(**it),std::make_shared<const string>(), true);
      if(!istatus.ok() && istatus.statusCode() != StatusCode::REMOTE_NOT_FOUND) 
        kio_warning("Failed removing indicator key for target-key \"", **it, "\" ", istatus);
      
      if((o == Operation::REPAIR && !need_action) || (o == Operation::RESET && indicator_keys))
        continue;
      
      if (getstatus.ok()) {
        auto putstatus = this->put(*it, version, value, false, version);
        if (!putstatus.ok())
          throw kio_exception(EIO, "Failed put operation on target-key \"", **it, "\" ", putstatus);
        key_counts.repaired++;
      }
      else if (getstatus.statusCode() == StatusCode::REMOTE_NOT_FOUND) {
        auto rmstatus = this->remove(*it, version, true);
        if (!rmstatus.ok())
          throw kio_exception(EIO, "Failed remove operation on target-key \"", **it, "\" ", rmstatus);
        key_counts.removed++;
      }
      else
        throw kio_exception(EIO, "Failed get operation on target-key \"", **it, "\" ", getstatus);

    } catch (const std::exception& e) {
      key_counts.unrepairable++;
      kio_warning(e.what());
    }
  }
}

int KineticAdminCluster::doOperation(Operation o, size_t maximum, bool restart)
{
  std::unique_ptr<std::vector<std::string>> keys;
  
  /* Set up key ranges. */
  auto end_key = std::make_shared<const std::string>(1, static_cast<char>(255)); 
  if(indicator_keys)
     end_key = utility::keyToIndicator(*end_key);
  if (!start_key || restart){
    bg.reset(new BackgroundOperationHandler(threads, threads));
    key_counts.incomplete = key_counts.need_repair = key_counts.removed = key_counts.repaired
        = key_counts.total = key_counts.unrepairable = 0;
    if(indicator_keys)
      start_key = utility::keyToIndicator("");
    else
      start_key = std::make_shared<const string>("/");
  }
  
  /* Iterate over up to to maximum keys and perform the requested operation */
  int processed_keys = 0;
  do {
    auto status = range(start_key, end_key, maximum - processed_keys > 100 ? 100 : maximum - processed_keys, keys);
    if (!status.ok()) {
      kio_warning("range(", *start_key, " - ", *end_key, ") failed on cluster. Cannot proceed. ", status);
      break;
    }
    if (keys && keys->size()) {
      start_key = std::make_shared<const std::string>(keys->back() + static_cast<char>(0));
      key_counts.total += keys->size();
      processed_keys += keys->size();
      
      if (o != Operation::COUNT) {
        std::vector<std::shared_ptr<const std::string>> out;
        for (auto it = keys->cbegin(); it != keys->cend(); it++){
          if(indicator_keys)
            out.push_back(utility::indicatorToKey(*it));
          else
            out.push_back(std::make_shared<const string>(std::move(*it)));
        }
        bg->run(std::bind(&KineticAdminCluster::applyOperation, this, o, out));
      }
    }
  } while (keys && keys->size() && processed_keys < maximum);
  return processed_keys; 
}

int KineticAdminCluster::count(size_t maximum, bool restart)
{
  return doOperation(Operation::COUNT, maximum, restart);
}

int KineticAdminCluster::scan(size_t maximum, bool restart)
{
  return doOperation(Operation::SCAN, maximum, restart);
}

int KineticAdminCluster::repair(size_t maximum, bool restart)
{
  return doOperation(Operation::REPAIR, maximum, restart);
}

int KineticAdminCluster::reset(size_t maximum, bool restart)
{
  return doOperation(Operation::RESET, maximum, restart);
}

AdminClusterInterface::KeyCounts KineticAdminCluster::getCounts()
{
  /* Re-initialize background operation handler to ensure that all possibly scheduled operations
   complete before returning key counts. */
  bg.reset(new BackgroundOperationHandler(threads, threads));
  return KeyCounts{key_counts.total, key_counts.incomplete, key_counts.need_repair, 
          key_counts.repaired, key_counts.removed, key_counts.unrepairable};
}
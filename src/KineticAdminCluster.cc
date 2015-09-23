#include "KineticAdminCluster.hh"
#include <memory>
#include <string>
#include <Logging.hh>

using namespace kio;
using namespace kinetic;


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

bool KineticAdminCluster::needsRepair(const std::shared_ptr<const std::string>& key, KeyCountsInternal& counts)
{
  auto ops = initialize(key, nData + nParity);
  auto sync = asyncops::fillGetVersion(ops, key);
  auto rmap = execute(ops, *sync);

  auto valid_results = rmap[StatusCode::OK] + rmap[StatusCode::REMOTE_NOT_FOUND];
  auto target_version = asyncops::mostFrequentVersion(ops);

  if(valid_results < nData+nParity){
    kio_debug("Not all drives for key \"", *key, "\" could be accessed: Only ", valid_results, " valid results for a "
        "stripe size of ", nData+nParity);
    counts.incomplete++;
  }

  if(target_version.frequency == valid_results && valid_results >= nData) {
    kio_debug("Key \"", *key, "\" does not require repair.");
    return false;
  }
  else if(target_version.frequency >=  nData) {
    kio_debug("Key \"", *key, "\" requires repair.");

    counts.need_repair++;
    return true;
  }

  throw std::runtime_error(utility::Convert::toString(
      "Key ", *key, " is unrepairable. We succeeded to read ", valid_results, " of ", nData+nParity,
      " key versions, ", target_version.frequency, " of which have an equivalent version (", nData, ") needed."
  ));
}


void KineticAdminCluster::scanAndRepair(std::vector<std::shared_ptr<const std::string>> keys, Operation o, KeyCountsInternal& counts)
{
  auto version = std::make_shared<const string>("");
  auto value = std::make_shared<const string>("");
  for(auto it = keys.cbegin(); it != keys.cend(); it++) {
    try {
      if (needsRepair(*it, counts) && o == Operation::REPAIR) {
        auto getstatus = this->get(*it, false, version, value);
        if(getstatus.ok()){
          auto putstatus = this->put(*it, version, value, false, version);
          if (!putstatus.ok())
            throw kio_exception(EIO, "Failed put operation on repair target-key \"", **it, "\" ", putstatus);
          counts.repaired++;
        }
        else if(getstatus.statusCode() == StatusCode::REMOTE_NOT_FOUND){
          auto rmstatus = this->remove(*it,version,true);
          if (!rmstatus.ok())
            throw kio_exception(EIO, "Failed remove operation on repair target-key \"", **it, "\" ", rmstatus);
          counts.removed++;
        }
        else
          throw kio_exception(EIO, "Failed get operation on repair target-key \"", **it, "\" ", getstatus);
      }
    } catch (const std::exception& e) {
      counts.unrepairable++;
      kio_warning(e.what());
    }
  }
}

KineticAdminCluster::KeyCounts KineticAdminCluster::doOperation(Operation o) {
  KeyCountsInternal c;
  {
  std::unique_ptr<std::vector<std::string>> keys;
  BackgroundOperationHandler background(connections.size() / 2, connections.size());

  auto start_key = std::make_shared<const std::string>(1, static_cast<char>(0));
  auto end_key = std::make_shared<const std::string>(1, static_cast<char>(255));
  do {
    auto status = range(start_key, end_key, 100, keys);
    if (!status.ok()) {
      kio_warning("range() failed on cluster. Cannot proceed. ", status);
      break;
    }
    if (keys && keys->size()) {
      start_key = std::make_shared<const std::string>(keys->back() + static_cast<char>(0));
      c.total += keys->size();

      if (o != Operation::COUNT) {
        std::vector<std::shared_ptr<const std::string>> out;
        for (auto it = keys->cbegin(); it != keys->cend(); it++)
          out.push_back(std::make_shared<const string>(std::move(*it)));

        auto b = std::bind(&KineticAdminCluster::scanAndRepair, this, out, o, std::ref(c));
        background.run(b);
      }
    }
  } while (keys && keys->size());

  background.drain_queue();
  }
  return KeyCounts{c.total, c.incomplete, c.need_repair, c.repaired, c.removed, c.unrepairable};
}

int KineticAdminCluster::count()
{
  return doOperation(Operation::COUNT).total;
}

KineticAdminCluster::KeyCounts KineticAdminCluster::scan()
{
  return doOperation(Operation::SCAN);
}

KineticAdminCluster::KeyCounts KineticAdminCluster::repair()
{
  return doOperation(Operation::REPAIR);
}
#include <stdlib.h>
#include <fstream>
#include <sstream>
#include "Logging.hh"
#include "ClusterMap.hh"
#include <iostream>

using namespace kio;


/* Printing errors initializing static global object to stderr.*/
ClusterMap::ClusterMap() : listener(new SocketListener())
{
}

void ClusterMap::reset(
  std::unordered_map<std::string,ClusterInformation> clusterInfo, 
  std::unordered_map<std::string,std::pair<kinetic::ConnectionOptions,kinetic::ConnectionOptions> > driveInfo
)
{
  std::lock_guard<std::mutex> locker(mutex);
  clusterInfoMap = std::move(clusterInfo);
  driveInfoMap = std::move(driveInfo);
  ecClusterCache.clear();
  replClusterCache.clear();
}

void ClusterMap::fillArgs(const ClusterInformation& ki, const RedundancyType& rType,
                          std::shared_ptr<RedundancyProvider>& rp,
                          std::vector<std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>>& cops)
{
  for (auto wwn = ki.drives.begin(); wwn != ki.drives.end(); wwn++) {
    if (!driveInfoMap.count(*wwn))
      throw kio_exception(ENODEV, "Nonexisting drive wwn requested: ", *wwn);
    cops.push_back(driveInfoMap.at(*wwn));
  }

  auto rpName = utility::Convert::toString((rType == RedundancyType::ERASURE_CODING) ? ki.numData : 1, "-", ki.numParity);
  if(rpCache.count(rpName)){
    rp = rpCache.at(rpName);
  }
  else{
    rp = std::make_shared<RedundancyProvider>((rType == RedundancyType::ERASURE_CODING) ? ki.numData : 1, ki.numParity);
    rpCache.insert(std::make_pair(rpName, rp));
  }
}

std::unique_ptr<KineticAdminCluster> ClusterMap::getAdminCluster(const std::string& id, RedundancyType r)
{
  std::lock_guard<std::mutex> locker(mutex);
  if (!clusterInfoMap.count(id))
    throw kio_exception(ENODEV, "Nonexisting cluster id requested: ", id);

  std::vector<std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>> cops;
  std::shared_ptr<RedundancyProvider> rp;
  ClusterInformation &ki = clusterInfoMap.at(id);
  fillArgs(ki, r, rp, cops);

  return std::unique_ptr<KineticAdminCluster>(new KineticAdminCluster(
      id, (r == RedundancyType::ERASURE_CODING) ? ki.numData : 1, ki.numParity, ki.blockSize,
      cops, ki.min_reconnect_interval, ki.operation_timeout,
      rp, *listener)
  );
}

std::shared_ptr<ClusterInterface> ClusterMap::getCluster(const std::string &id, RedundancyType r)
{
  std::lock_guard<std::mutex> locker(mutex);
  if (!clusterInfoMap.count(id))
    throw kio_exception(ENODEV, "Nonexisting cluster id requested: ", id);
  
  auto& cache = (r == RedundancyType::ERASURE_CODING) ? ecClusterCache : replClusterCache;
    
  if(!cache.count(id)){
    ClusterInformation &ki = clusterInfoMap.at(id);
    std::vector<std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>> cops;
    std::shared_ptr<RedundancyProvider> rp;
    fillArgs(ki, r, rp, cops);

    cache.insert(
      std::make_pair(id, 
        std::make_shared<KineticCluster>(
          id, (r == RedundancyType::ERASURE_CODING) ? ki.numData : 1, ki.numParity, ki.blockSize,
          cops, ki.min_reconnect_interval, ki.operation_timeout,
          rp, *listener
      ))
    );
  }
  return cache.at(id);
}

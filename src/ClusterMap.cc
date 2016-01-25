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
    std::unordered_map<std::string, ClusterInformation> clusterInfo,
    std::unordered_map<std::string, std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions> > driveInfo
)
{
  std::lock_guard<std::mutex> locker(mutex);
  clusterInfoMap = std::move(clusterInfo);
  driveInfoMap = std::move(driveInfo);
  clusterCache.clear();
}

std::shared_ptr<AdminClusterInterface> ClusterMap::getAdminCluster(const std::string& id)
{
  /* will throw if cluster does not exist */
  getCluster(id);
  return clusterCache.at(id);
}

std::shared_ptr<ClusterInterface> ClusterMap::getCluster(const std::string& id)
{
  std::lock_guard<std::mutex> locker(mutex);
  if (clusterCache.count(id)) {
    return clusterCache.at(id);
  }

  if (!clusterInfoMap.count(id)) {
    kio_warning("Nonexisting cluster id requested: ", id);
    throw std::system_error(std::make_error_code(std::errc::no_such_device));
  }
  ClusterInformation& ki = clusterInfoMap.at(id);

  /* Build a connection vector for the cluster */
  std::vector<std::unique_ptr<KineticAutoConnection>> connections;
  for (auto wwn = ki.drives.begin(); wwn != ki.drives.end(); wwn++) {
    if (!driveInfoMap.count(*wwn)) {
      kio_warning("Nonexisting drive wwn requested: ", *wwn);
      throw std::system_error(std::make_error_code(std::errc::no_such_device));
    }
    std::unique_ptr<KineticAutoConnection> autocon(
        new KineticAutoConnection(*listener, driveInfoMap.at(*wwn), ki.min_reconnect_interval)
    );
    connections.push_back(std::move(autocon));
  }

  /* Get data and metadata redundancy providers */
  auto rpDataName = utility::Convert::toString(ki.numData, "-", ki.numParity);
  auto rpMetadataName = utility::Convert::toString("1-", ki.numParity);
  if (!rpCache.count(rpDataName)) {
    rpCache.insert(std::make_pair(rpDataName, std::make_shared<RedundancyProvider>(ki.numData, ki.numParity)));
  }
  if (!rpCache.count(rpMetadataName)) {
    rpCache.insert(std::make_pair(rpMetadataName, std::make_shared<RedundancyProvider>(1, ki.numParity)));
  }

  clusterCache.insert(
      std::make_pair(id,
                     std::make_shared<KineticAdminCluster>(
                         id, ki.blockSize, ki.operation_timeout, std::move(connections),
                         rpCache.at(rpDataName), rpCache.at(rpMetadataName)
                     ))
  );

  return clusterCache.at(id);
}

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

#include <unistd.h>
#include "KineticAdminCluster.hh"
#include "SimulatorController.h"
#include "Utility.hh"
#include "catch.hpp"
#include "KineticIoSingleton.hh"

using std::shared_ptr;
using std::string;
using std::make_shared;
using namespace kinetic;
using namespace kio;

bool indicator_exists(std::shared_ptr<ClusterInterface> cluster)
{
  std::unique_ptr<std::vector<string>> keys;
  auto status = cluster->range(
      utility::makeIndicatorKey(""),
      utility::makeIndicatorKey("~"),
      keys);
  REQUIRE(status.ok());
  return !keys->empty();
}

bool handoff_exists(std::shared_ptr<ClusterInterface> cluster)
{
  std::unique_ptr<std::vector<string>> keys;
  auto status = cluster->range(
      std::make_shared<const std::string>("hand"),
      std::make_shared<const std::string>("hand~"),
      keys);
  REQUIRE(status.ok());
  return !keys->empty();
}

SCENARIO("Repair test.", "[Repair]") 
{
  auto& c = SimulatorController::getInstance();
  SocketListener listener;

  GIVEN ("A valid admin cluster") {
    REQUIRE(c.reset());

    std::string clusterId = "testCluster";
    std::size_t blocksize = 1024 * 1024;

    std::vector<std::unique_ptr<KineticAutoConnection>> connections;
    for (int i = 0; i < 10; i++) {
      std::unique_ptr<KineticAutoConnection> autocon(
          new KineticAutoConnection(listener, std::make_pair(c.get(i), c.get(i)), std::chrono::seconds(1))
      );
      connections.push_back(std::move(autocon));
    }

    auto cluster = std::make_shared<KineticAdminCluster>(clusterId, blocksize, std::chrono::seconds(10),
                                                         std::move(connections),
                                                         std::make_shared<RedundancyProvider>(6, 4)
    );
   
    WHEN("Putting a data and metadata key") {                    
      shared_ptr<const string> putversion;
      
      auto value = make_shared<const string>(cluster->limits().max_value_size, 'v');
      
      auto status = cluster->put(
          utility::makeDataKey(clusterId, "key", 1),
          value,
          putversion);
      REQUIRE(status.ok());
      REQUIRE(putversion);
           
      status = cluster->put(
          utility::makeMetadataKey(clusterId, "key"),
          make_shared<const string>(cluster->limits().max_value_size, 'v'),
          putversion);
      REQUIRE(status.ok());
      REQUIRE(putversion);
            
      THEN("Keys should be repairable after resetting nParity drives") {
        for(int i=0; i<4; i++) {
          c.reset(i);
        }
        auto repair = cluster->repair(AdminClusterInterface::OperationTarget::DATA);
        REQUIRE(repair.repaired == 1);     
        
        std::shared_ptr<const std::string> get_value; 
        std::shared_ptr<const std::string> get_version; 
          
        cluster->get(utility::makeDataKey(clusterId, "key", 1), get_version, get_value);
        REQUIRE(*value == *get_value);
      
        repair = cluster->repair(AdminClusterInterface::OperationTarget::METADATA);
        REQUIRE(repair.repaired == 1);  
      }
      
      THEN("Keys should not be repairable after resetting nParity+1 drives") {
        for(int i=0; i<5; i++) {
          c.reset(i);
        }
        auto repair = cluster->repair(AdminClusterInterface::OperationTarget::METADATA); 
        REQUIRE(repair.unrepairable == 1);
        
        repair = cluster->repair(AdminClusterInterface::OperationTarget::DATA);
        REQUIRE(repair.unrepairable == 1);
        
      }
      
      THEN("Keys should be detected as partial deletes after resetting > (nData+nParity)/2 drives") {
        for(int i=0; i<6; i++) {
          c.reset(i);
        }
        auto repair = cluster->repair(AdminClusterInterface::OperationTarget::METADATA);
        REQUIRE(repair.removed == 1);     
        
        repair = cluster->repair(AdminClusterInterface::OperationTarget::DATA);
        REQUIRE(repair.removed == 1);     
      }
     
    }
  }
}


SCENARIO("Admin integration test.", "[Admin]")
{
  auto& c = SimulatorController::getInstance();
  SocketListener listener;

  GIVEN ("A valid admin cluster") {
    REQUIRE(c.reset());

    std::string clusterId = "testCluster";
    std::size_t nData = 2;
    std::size_t nParity = 1;
    std::size_t blocksize = 1024 * 1024;

    std::vector<std::unique_ptr<KineticAutoConnection>> connections;
    for (int i = 0; i < 3; i++) {
      std::unique_ptr<KineticAutoConnection> autocon(
          new KineticAutoConnection(listener, std::make_pair(c.get(i), c.get(i)), std::chrono::seconds(1))
      );
      connections.push_back(std::move(autocon));
    }

    auto cluster = std::make_shared<KineticAdminCluster>(clusterId, blocksize, std::chrono::seconds(10),
                                                         std::move(connections),
                                                         std::make_shared<RedundancyProvider>(nData, nParity)
    );


    WHEN("Putting a key-value pair with one drive down") {
      c.block(0);

      auto target = AdminClusterInterface::OperationTarget::INVALID;
      std::shared_ptr<const std::string> key;
      
      int i = rand() % 3;

      if (i == 0) {
        target = AdminClusterInterface::OperationTarget::DATA;
        key = utility::makeDataKey(clusterId, "key", 1);
      }
      else if (i == 1) {
        target = AdminClusterInterface::OperationTarget::ATTRIBUTE;
        key = utility::makeAttributeKey(clusterId, "key", "attribute");
      }
      else {
        target = AdminClusterInterface::OperationTarget::METADATA;
        key = utility::makeMetadataKey(clusterId, "key");
      }

      shared_ptr<const string> putversion;
      auto status = cluster->put(
          key,
          make_shared<const string>(cluster->limits().max_value_size, 'v'),
          putversion);
      REQUIRE(status.ok());
      REQUIRE(putversion);

      THEN("It is marked as incomplete during a scan") {
        auto kc = cluster->scan(target);
        REQUIRE((kc.total == 1));
        REQUIRE((kc.incomplete == 1));
        REQUIRE((kc.need_action == 0));
        REQUIRE((kc.removed == 0));
        REQUIRE((kc.repaired == 0));
        REQUIRE((kc.unrepairable == 0));
      }
      THEN("An indicator key and a handoff key will have been generated.") {
        REQUIRE(indicator_exists(cluster));
        REQUIRE(handoff_exists(cluster));
      }
      THEN("We can't repair it while the drive is down.") {
        auto kc = cluster->repair(target);
        REQUIRE((kc.repaired == 0));
        AND_THEN("The repair attempt should not remove existing indicator or handoff keys."){
          REQUIRE(indicator_exists(cluster));
          REQUIRE(handoff_exists(cluster));
        }
      }
      THEN("We can still remove it by resetting the cluster.") {
        auto kc = cluster->reset(target);
        REQUIRE((kc.removed == 1));
        AND_THEN("Target reset should not remove existing indicator or handoff keys."){
          REQUIRE(indicator_exists(cluster));
          REQUIRE(handoff_exists(cluster));
          AND_THEN("Attempting to repair will lead to removal."){
            auto kc = cluster->repair(AdminClusterInterface::OperationTarget::INDICATOR);
            REQUIRE((kc.removed == 1));
            REQUIRE(!indicator_exists(cluster));
            REQUIRE(!handoff_exists(cluster));
          }
        }
      }

      AND_WHEN("The drive comes up again.") {
        c.enable(0);

        THEN("It is no longer marked as incomplete but as need_action after a scan") {
          auto kc = cluster->scan(target);
          REQUIRE((kc.total == 1));
          REQUIRE((kc.incomplete == 0));
          REQUIRE((kc.need_action == 1));
          REQUIRE((kc.removed == 0));
          REQUIRE((kc.repaired == 0));
          REQUIRE((kc.unrepairable == 0));
        }
        THEN("We can repair the key providing the key type as target.") {
          auto kc = cluster->repair(target);
          REQUIRE((kc.repaired == 1));
          AND_THEN("The associated indicator and handoff keys should be removed after successfull repair") {
            REQUIRE(!indicator_exists(cluster));
            REQUIRE(!handoff_exists(cluster));
          }
        }
        THEN("We can repair the key providing indicator as target.") {
          auto kc = cluster->repair(AdminClusterInterface::OperationTarget::INDICATOR);
          REQUIRE((kc.repaired == 1));
          AND_THEN("The associated indicator and handoff keys should be removed after successfull repair") {
            REQUIRE(!indicator_exists(cluster));
            REQUIRE(!handoff_exists(cluster));
          }
        }
        THEN("We can reset the cluster.") {
          auto kc = cluster->reset(target);
          REQUIRE((kc.removed == 1));
        }
      }
    }
  }
}

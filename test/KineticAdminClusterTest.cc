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
#include "catch.hpp"
#include "KineticAdminCluster.hh"
#include "SimulatorController.h"
#include "Utility.hh"

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
      keys,
      KeyType::Data);
  REQUIRE(status.ok());
  return !keys->empty();
}

bool handoff_exists(std::shared_ptr<ClusterInterface> cluster)
{
  std::unique_ptr<std::vector<string>> keys;
  auto status = cluster->range(
      std::make_shared<const std::string>("hand"),
      std::make_shared<const std::string>("hand~"),
      keys,
      KeyType::Data);
  REQUIRE(status.ok());
  return !keys->empty();
}


SCENARIO("Admin integration test.", "[Admin]")
{
  auto& c = SimulatorController::getInstance();
  c.start(0);
  c.start(1);
  c.start(2);

  SocketListener listener;

  GIVEN ("A valid admin cluster") {
    REQUIRE(c.reset(0));
    REQUIRE(c.reset(1));
    REQUIRE(c.reset(2));

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
                                                         std::make_shared<RedundancyProvider>(nData, nParity),
                                                         std::make_shared<RedundancyProvider>(1, nParity+1)
    );


    WHEN("Putting a key-value pair with one drive down") {
      c.block(0);

      auto target = AdminClusterInterface::OperationTarget::INVALID;
      std::shared_ptr<const std::string> key;
      KeyType type;
      int i = rand() % 3;

      if (i == 0) {
        target = AdminClusterInterface::OperationTarget::DATA;
        key = utility::makeDataKey(clusterId, "key", 1);
        type = KeyType::Data;
      }
      else if (i == 1) {
        target = AdminClusterInterface::OperationTarget::ATTRIBUTE;
        key = utility::makeAttributeKey(clusterId, "key", "attribute");
        type = KeyType::Metadata;
      }
      else if (i == 2) {
        target = AdminClusterInterface::OperationTarget::METADATA;
        key = utility::makeMetadataKey(clusterId, "key");
        type = KeyType::Metadata;
      }

      shared_ptr<const string> putversion;
      auto status = cluster->put(
          key,
          make_shared<const string>(cluster->limits(type).max_value_size, 'v'),
          putversion,
          type);
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
            REQUIRE(kc.removed == 1);
            REQUIRE(!indicator_exists(cluster));
            REQUIRE(!handoff_exists(cluster));
          }
        }
      }

      AND_WHEN("The drive comes up again.") {
        c.start(0);

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

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
#include "KineticCluster.hh"
#include "SimulatorController.h"
#include "Utility.hh"
#include "catch.hpp"

using std::shared_ptr;
using std::string;
using std::make_shared;
using namespace kinetic;
using namespace kio;

SCENARIO("Cluster integration test.", "[Cluster]")
{

  auto& c = SimulatorController::getInstance();
  SocketListener listener;

  GIVEN ("A valid drive cluster") {
    REQUIRE(c.reset(0));
    REQUIRE(c.reset(1));
    REQUIRE(c.reset(2));

    std::size_t nData = 2;
    std::size_t nParity = 1;
    std::size_t blocksize = 1024 * 1024;

    std::vector<std::unique_ptr<KineticAutoConnection>> connections;
    for (int i = 0; i < 3; i++) {
      std::unique_ptr<KineticAutoConnection> autocon(
          new KineticAutoConnection(listener, std::make_pair(c.get(i), c.get(i)), std::chrono::seconds(10))
      );
      connections.push_back(std::move(autocon));
    }
    auto cluster = std::make_shared<KineticCluster>("testcluster", blocksize, std::chrono::seconds(10),
                                                    std::move(connections),
                                                    std::make_shared<RedundancyProvider>(nData, nParity)
    );

    THEN("cluster limits reflect kinetic-drive limits") {
      REQUIRE((cluster->limits().max_key_size == 4096));
      REQUIRE((cluster->limits().max_value_size == nData * 1024 * 1024));
    }

    WHEN("Putting a key-value pair on a cluster with 1 drive failure") {
      auto key = utility::makeDataKey(cluster->id(), "key", 0);
      auto value = make_shared<string>("this is a value");
      c.block(0);

      shared_ptr<const string> putversion;
      auto status = cluster->put(
          key,
          value,
          putversion);
      REQUIRE(status.ok());
      REQUIRE(putversion);

      THEN("An indicator key will have been generated") {
        std::unique_ptr<std::vector<string>> keys;
        auto status = cluster->range(
            utility::makeIndicatorKey(cluster->id()+""),
            utility::makeIndicatorKey(cluster->id()+"~"),
            keys);
        REQUIRE(status.ok());
        REQUIRE((keys->size() == 1));

        AND_THEN("Indicator existence is reflected in stats"){
          cluster->stats();
          usleep(500*1000);
          auto stats = cluster->stats();
          REQUIRE(stats.health.indicator_exist);
        }
      }

      THEN("A handoff key will have been generated") {
        std::unique_ptr<std::vector<string>> keys;
        auto status = cluster->range(
            std::make_shared<const std::string>("hand"),
            std::make_shared<const std::string>("hand~"),
            keys);
        REQUIRE(status.ok());
        REQUIRE((keys->size() == 1));

        AND_THEN("We can read it again") {
          shared_ptr<const string> getversion;
          shared_ptr<const string> getvalue;
          auto status = cluster->get(key, getversion, getvalue);
          REQUIRE(status.ok());
          REQUIRE((*getversion == *putversion));
          REQUIRE((*getvalue == *value));

          AND_THEN("We should be able to successfully read in the value even with > nParity drive failures") {
            /* We just know which drive the handoff key landed on. */
            c.block(1);
            shared_ptr<const string> getversion;
            shared_ptr<const string> getvalue;
            auto status = cluster->get(key, getversion, getvalue);
            REQUIRE(status.ok());
            REQUIRE((*getversion == *putversion));
            REQUIRE((*getvalue == *value));
          }
        }
      }
    }

    WHEN("Some keys have been put down") {

      shared_ptr<const string> putversion;
      for (int i = 0; i < 10; i++) {
        auto status = cluster->put(
            make_shared<string>(utility::Convert::toString("key", i)),
            make_shared<string>("value"),
            putversion);
        REQUIRE(status.ok());
      }
      THEN("range will work and respect max_elements") {
        std::unique_ptr<std::vector<std::string>> keys;
        auto status = cluster->range(make_shared<string>("key"), make_shared<string>("key9"), keys, 3);
        REQUIRE(status.ok());
        REQUIRE((keys->size() == 3));
        status = cluster->range(make_shared<string>("key"), make_shared<string>("key9"), keys);
        REQUIRE(status.ok());
        REQUIRE((keys->size() == 10));
        status = cluster->range(make_shared<string>("key5"), make_shared<string>("key9"), keys);
        REQUIRE(status.ok());
        REQUIRE((keys->size() == 5));
      }

    }

    WHEN("Putting a key-value pair on a healthy cluster") {
      auto value = make_shared<string>(66, 'v');

      shared_ptr<const string> putversion;
      auto status = cluster->put(
          make_shared<string>("key"),
          value,
          putversion);
      REQUIRE(status.ok());
      REQUIRE(putversion);

      THEN("After nParity drive failures.") {
        for (size_t i = 0; i < nParity; i++) {
          c.block(i);
        }

        AND_WHEN("The key is read in again") {
          std::shared_ptr<const string> getvalue;
          std::shared_ptr<const string> version;

          auto status = cluster->get(make_shared<string>("key"), version, getvalue);
          REQUIRE(status.ok());
          REQUIRE(status.ok());
          REQUIRE(version);
          REQUIRE((*version == *putversion));
          REQUIRE(getvalue);
          REQUIRE((*getvalue == *value));

          THEN("An indicator key will _not_ have been generated") {
            std::unique_ptr<std::vector<string>> keys;
            auto status = cluster->range(
                utility::makeIndicatorKey(""),
                utility::makeIndicatorKey("~"),
                keys
            );
            REQUIRE(status.ok());
            REQUIRE((keys->size() == 0));
          }
        }

        THEN("Removing it with the correct version succeeds") {
          auto status = cluster->remove(
              make_shared<string>("key"),
              putversion);
          REQUIRE(status.ok());

          AND_WHEN("The missing drives are available again.") {
            for (size_t i = 0; i < nParity; i++) {
              c.enable(i);
            }

            THEN("The key is still considered not available.") {
              shared_ptr<const string> version;
              shared_ptr<const string> readvalue;
              auto status = cluster->get(
                  make_shared<string>("key"),
                  version,
                  readvalue);
              REQUIRE((status.statusCode() == StatusCode::REMOTE_NOT_FOUND));
            }
          }
        }

        THEN("It can be overwritten.") {
          auto newval = make_shared<string>(cluster->limits().max_value_size, 'x');
          shared_ptr<const string> newver;
          auto status = cluster->put(
              make_shared<string>("key"),
              putversion,
              newval,
              newver);
          REQUIRE(status.ok());
          REQUIRE(newver);
        }


        THEN("It can be read in again ") {
          shared_ptr<const string> version;
          shared_ptr<const string> readvalue;
          auto status = cluster->get(
              make_shared<string>("key"),
              version,
              readvalue);
          REQUIRE(status.ok());
          REQUIRE(version);
          REQUIRE((*version == *putversion));
          REQUIRE(readvalue);
          REQUIRE((*readvalue == *value));
        }

        THEN("Removing it with an incorrect version fails with INVALID_VERSION Status Code") {
          auto status = cluster->remove(
              make_shared<string>("key"),
              make_shared<string>("incorrect"));
          REQUIRE((status.statusCode() == kinetic::StatusCode::REMOTE_VERSION_MISMATCH));
        }

        THEN("with > nParity failures") {
          c.block(nParity);
          THEN("It can't be read again") {
            shared_ptr<const string> version;
            shared_ptr<const string> readvalue;
            auto status = cluster->get(
                make_shared<string>("key"),
                version,
                readvalue);
            REQUIRE((status.statusCode() == StatusCode::CLIENT_IO_ERROR));
          }
        }
      }
    }

    THEN("Cluster size is reported as long as a single drive is alive.") {
      for (size_t i = 0; i < nData + nParity + 1; i++) {
        cluster->stats();
        // sleep so that previous cluster->size background thread may finish
        usleep(500 * 1000);
        ClusterStats s = cluster->stats();
        REQUIRE((s.health.drives_failed == i));
        if (i == nData + nParity) {
          REQUIRE((s.bytes_total == 0));
        }
        else {
          REQUIRE((s.bytes_total > 0));
          c.block(i);
          usleep(2500 * 1000);
        }
      }
    }
  }
}

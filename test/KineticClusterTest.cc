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
#include "KineticCluster.hh"
#include "SimulatorController.h"
#include "Utility.hh"

using std::shared_ptr;
using std::string;
using std::make_shared;
using namespace kinetic;
using namespace kio;

SCENARIO("Cluster integration test.", "[Cluster]")
{

  auto& c = SimulatorController::getInstance();
  c.start(0);
  c.start(1);
  c.start(2);

  SocketListener listener;

  GIVEN ("A valid drive cluster") {
    REQUIRE(c.reset(0));
    REQUIRE(c.reset(1));
    REQUIRE(c.reset(2));

    std::vector<std::pair<ConnectionOptions, ConnectionOptions> > info;
    info.push_back(std::pair<ConnectionOptions, ConnectionOptions>(c.get(0), c.get(0)));
    info.push_back(std::pair<ConnectionOptions, ConnectionOptions>(c.get(1), c.get(1)));
    info.push_back(std::pair<ConnectionOptions, ConnectionOptions>(c.get(2), c.get(2)));

    std::size_t nData = 2;
    std::size_t nParity = 1;
    std::size_t blocksize = 1024*1024;

    auto cluster = make_shared<KineticCluster>("testcluster", nData, nParity, blocksize, info,
                                               std::chrono::seconds(20),
                                               std::chrono::seconds(2),
                                               std::make_shared<RedundancyProvider>(nData, nParity),
                                               listener
    );

    THEN("cluster limits reflect kinetic-drive limits") {
      REQUIRE(cluster->limits().max_key_size == 4096);
      REQUIRE(cluster->limits().max_value_size == nData * 1024 * 1024);
    }

    THEN("Cluster size is reported as long as a single drive is alive.") {
      for (int i = 0; i < nData + nParity + 1; i++) {
        cluster->size();
        // sleep so that previous cluster->size background thread may finish
        usleep(2500 * 1000);
        ClusterSize s = cluster->size();
        if (i == nData + nParity) {
          REQUIRE(s.bytes_total == 0);
        }
        else {
          REQUIRE(s.bytes_total > 0);
          c.stop(i);
        }
      }
    }

    WHEN("Putting a key-value pair on a cluster with 1 drive failure") {
      auto value = make_shared<string>(cluster->limits().max_value_size, 'v');
      c.stop(0);

      shared_ptr<const string> putversion;
      auto status = cluster->put(
          make_shared<string>("key"),
          make_shared<string>("version"),
          value,
          true,
          putversion);
      REQUIRE(status.ok());
      REQUIRE(putversion);

      THEN("An indicator key will have been generated") {
        std::unique_ptr<std::vector<string>> keys;
        auto status = cluster->range(
            utility::makeIndicatorKey(""),
            utility::makeIndicatorKey("~"),
            100,
            keys
        );
        REQUIRE(status.ok());
        REQUIRE(keys->size() == 1);
      }
     }

    WHEN("Putting a key-value pair on a healthy cluster") {
      auto value = make_shared<string>(666, 'v');

      shared_ptr<const string> putversion;
      auto status = cluster->put(
          make_shared<string>("key"),
          make_shared<string>("version"),
          value,
          true,
          putversion);
      REQUIRE(status.ok());
      REQUIRE(putversion);

      THEN("After nParity drive failures.") {
        for (int i = 0; i < nParity; i++)
          c.stop(i);
        
        AND_WHEN("The key is read in again"){
            std::shared_ptr<const string> getvalue;
            std::shared_ptr<const string> version;
            
            auto status = cluster->get(make_shared<string>("key"), false, version, getvalue);
            REQUIRE(status.ok());
            REQUIRE(status.ok());
            REQUIRE(version);
            REQUIRE(*version == *putversion);
            REQUIRE(getvalue);
            REQUIRE(*getvalue == *value);
            
            THEN("An indicator key will _not_ have been generated"){
                std::unique_ptr<std::vector<string>> keys; 
                auto status = cluster->range(
                  utility::makeIndicatorKey(""),
                  utility::makeIndicatorKey("~"),
                  100,
                  keys
                );
                REQUIRE(status.ok());
                REQUIRE(keys->size() == 0);
            }
        }

        THEN("Removing it with the correct version succeeds") {
          auto status = cluster->remove(
              make_shared<string>("key"),
              putversion,
              false);
          REQUIRE(status.ok());

          AND_WHEN("The missing drives are available again.") {
            for (int i = 0; i < nParity; i++)
              c.start(i);

            THEN("The key is still considered not available.") {
              shared_ptr<const string> version;
              shared_ptr<const string> readvalue;
              auto status = cluster->get(
                  make_shared<string>("key"),
                  false,
                  version,
                  readvalue);
              REQUIRE(status.statusCode() == StatusCode::REMOTE_NOT_FOUND);
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
              false,
              newver);
          REQUIRE(status.ok());
          REQUIRE(newver);
        }


        THEN("It can be read in again ") {
          shared_ptr<const string> version;
          shared_ptr<const string> readvalue;
          auto status = cluster->get(
              make_shared<string>("key"),
              false,
              version,
              readvalue);
          REQUIRE(status.ok());
          REQUIRE(version);
          REQUIRE(*version == *putversion);
          REQUIRE(readvalue);
          REQUIRE(*readvalue == *value);
        }

        THEN("Removing it with an incorrect version fails with INVALID_VERSION Status Code") {
          auto status = cluster->remove(
              make_shared<string>("key"),
              make_shared<string>("incorrect"),
              false);
          REQUIRE(status.statusCode() == kinetic::StatusCode::REMOTE_VERSION_MISMATCH);
        }

        THEN("with > nParity failures") {
          c.stop(nParity);
          THEN("It can't be read again") {
            shared_ptr<const string> version;
            shared_ptr<const string> readvalue;
            auto status = cluster->get(
                make_shared<string>("key"),
                false,
                version,
                readvalue);
            REQUIRE(status.statusCode() == StatusCode::CLIENT_IO_ERROR);
          }
        }
      }
    }
  }
}

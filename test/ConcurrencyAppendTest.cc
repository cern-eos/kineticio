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

#include "catch.hpp"
#include "KineticCluster.hh"
#include "SimulatorController.h"
#include "Utility.hh"

using std::shared_ptr;
using std::string;
using std::make_shared;
using namespace kinetic;
using namespace kio;

void append_x(std::shared_ptr<KineticCluster> cluster, int data, int x)
{
  std::shared_ptr<const string> drive_version;
  std::shared_ptr<const string> new_version;
  auto strdata = utility::Convert::toString(data);
  auto key = make_shared<const string>("key");
  auto value = make_shared<const string>();

  for (int i = 0; i < x; i++) {
    while (true) {
      auto new_value = make_shared<const string>(*value + strdata);
      if (cluster->put(key, drive_version, new_value, new_version, KeyType::Data).ok())
        break;

      if (!cluster->get(key, drive_version, value, KeyType::Data).ok())
        throw std::runtime_error("Failed getting key.");
    }
  }
}


/* This test case is intended to verify that the target behaves consistently for write_back versioned puts
 * (no put_responses incorrectly show fail / success). */
SCENARIO("Append Concurrency Testing...", "[Append]")
{
  GIVEN ("A valid cluster configuration and one empty drive") {

    auto ec = std::make_shared<RedundancyProvider>(1, 0);
    SocketListener listener;

    auto& c = SimulatorController::getInstance();
    kinetic::ConnectionOptions conop = c.get(0);

    /* You may want to test a real drive instead of the simulator. just specify the drive's ip. */
    //kinetic::ConnectionOptions conop{"10.24.70.229", 8123, false, 1, "asdfasdf"};

    std::vector<std::shared_ptr<KineticCluster>> clusters;
    for (int instances = 1; instances <= 10; instances *= 10) {
      int numthreads = 11 - instances;
      WHEN(utility::Convert::toString(
          "We create ", instances, " instances of the same cluster (each with unique connections) "
          " and use ", numthreads, " threads per cluster instance to concurrently append data to a single key.")){

        for(int i=0; i<instances; i++) {
          std::vector<std::unique_ptr<KineticAutoConnection>> connections;
          connections.push_back(
              std::unique_ptr<KineticAutoConnection>(
                  new KineticAutoConnection(listener, std::make_pair(conop, conop), std::chrono::seconds(10)))
          );

          clusters.push_back(
              std::make_shared<KineticCluster>(
                  "testcluster",
                  1024 * 1024,
                  std::chrono::seconds(10),
                  std::move(connections),
                  ec, ec
              )
          );
        }

        /* if the key edxists, just throw it away */
        clusters[0]->remove(std::make_shared<const string>("key"), KeyType::Data);

        int x = 100;

        vector<std::thread> threads;
        for (int cl = 0; cl < (int)clusters.size(); cl++) {
          for (int t = 0; t < numthreads; t++) {
            threads.push_back(std::thread(std::bind(append_x, clusters[cl], cl+t, x)));
          }
        }
        for (size_t t = 0; t < threads.size(); t++) {
          threads[t].join();
        }

        /* Verification! */
        std::shared_ptr<const string> version;
        std::shared_ptr<const string> value;
        auto status = clusters[0]->get(std::make_shared<const string>("key"), version, value, KeyType::Data);

        REQUIRE(status.ok());
        REQUIRE((value->size() == clusters.size() * numthreads * x));

        std::map<char, int> result_counter;
        for(size_t i = 0; i < value->size(); i++){
          result_counter[value->at(i)]++;
        }

        for(int i=0; i < 10; i++){
          auto target = utility::Convert::toString(i)[0];
          REQUIRE((result_counter[target] == x));
        }
    }
  }
}
}


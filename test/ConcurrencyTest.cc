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
#include <DataBlock.hh>
#include "catch.hpp"
#include "KineticCluster.hh"
#include "SimulatorController.h"
#include "Utility.hh"
#include <Logging.hh>

using std::shared_ptr;
using std::string;
using std::make_shared;
using namespace kinetic;
using namespace kio;


void write_x(DataBlock& dataBlock, int x)
{
  std::string value = "value";
  for (int i = 0; i < x; i++) {
    dataBlock.write(value.c_str(), 0, value.length());
    dataBlock.flush();
  }
}

void delete_x(std::vector<std::shared_ptr<KineticCluster>>& clusters, int x)
{
  kio_notice("This is the deleter thread.");
  auto key = std::make_shared<const std::string>("key");

  for (int i = 0; i < x; i++) {
    auto cluster = clusters[rand() % clusters.size()];
    std::shared_ptr<const std::string> version;

    auto status = cluster->get(key, version, KeyType::Data);
    if (!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND)
      throw std::runtime_error("Get: unexpected result.");
    if (version) {
      status = cluster->remove(key, version, KeyType::Data);
      if (!status.ok() && status.statusCode() != StatusCode::REMOTE_VERSION_MISMATCH)
        throw std::runtime_error("Remove: unexpected result.");
    }
  }
}

SCENARIO("Kinetic Concurrency Testing...", "[Concurrency]")
{
  auto& c = SimulatorController::getInstance();
  SocketListener listener;

  /* Maybe make that configurable? */
  std::size_t nData = 2;
  std::size_t nParity = 1;
  std::size_t blocksize = 1024 * 1024;

  auto ec = std::make_shared<RedundancyProvider>(nData, nParity);
  auto repl = std::make_shared<RedundancyProvider>(1, nParity);

  GIVEN ("A valid cluster configuration and empty drives") {
    c.reset(0);
    c.reset(1);
    c.reset(2);

    for (int instances = 1; instances <= 16; instances *= 4) {
    for (int numblocks = 1; numblocks <= 8; numblocks *= 4) {
    WHEN(utility::Convert::toString(
        "We create ", instances, " instances of the same cluster and ", numblocks, " instances of DataBlock(s) ",
        "with the same key for each of the cluster instances"))
    {

        std::vector<std::shared_ptr<KineticCluster>> clusters;
        std::vector<std::unique_ptr<DataBlock>> dblocks;

        for (int inst = 0; inst < instances; inst++) {
          std::vector<std::unique_ptr<KineticAutoConnection>> connections;
          for (int con = 0; con < 3; con++) {
            std::unique_ptr<KineticAutoConnection> autocon(
                new KineticAutoConnection(listener, std::make_pair(c.get(con), c.get(con)),
                                          std::chrono::seconds(10))
            );
            connections.push_back(std::move(autocon));
          }

          clusters.push_back(std::make_shared<KineticCluster>(
              "testcluster", blocksize, std::chrono::seconds(10),
              std::move(connections), ec, repl
          ));

          for (int bl = 0; bl < numblocks; bl++) {
            dblocks.push_back(
                std::unique_ptr<DataBlock>(
                    new DataBlock(clusters.back(), std::make_shared<std::string>("key"), DataBlock::Mode::CREATE))
            );
          }
        }

        int numthreads = 2;
        REQUIRE((dblocks.size() * numthreads == (size_t) instances * numblocks * numthreads));
        const int total_put_operations = 1000;
        int x = total_put_operations / (dblocks.size()*numthreads);

        vector<std::thread> threads;
        for (auto it = dblocks.begin(); it != dblocks.end(); it++) {
          for (int t = 0; t < numthreads; t++) {
            threads.push_back(std::thread(std::bind(write_x, std::ref(*(it->get())), x)));
          }
        }
        /* Add a thread that removes the stripe from clusters */
        threads.push_back(std::thread(std::bind(delete_x, std::ref(clusters), x)));

        for (size_t t = 0; t < threads.size(); t++) {
          threads[t].join();
        }
        }
    }
    }
  }
}


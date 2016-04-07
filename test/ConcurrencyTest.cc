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

using std::shared_ptr;
using std::string;
using std::make_shared;
using namespace kinetic;
using namespace kio;


void write_100(DataBlock& dataBlock)
{
  std::string value = "value";
  try {
    for (int i = 0; i < 1000; i++) {
      dataBlock.write(value.c_str(), 0, value.length());
      dataBlock.flush();
    }
  }catch(const std::system_error& e){
    REQUIRE_NOTHROW(throw e);
  }
}


SCENARIO("Kinetic Concurrency Testing...", "[Concurrency]")
{
  auto& c = SimulatorController::getInstance();
  c.start(0);
  c.start(1);
  c.start(2);
  SocketListener listener;

  /* Maybe make that configurable?
   * also number instances of the cluster. */
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
      for (int numblocks = 1; numblocks <= 16; numblocks *= 4) {
        for (int numthreads = 1; numthreads <= 4; numthreads *= 4) {
          WHEN(utility::Convert::toString(
              "We create ", instances, " instances of the same cluster, ",
              numblocks, " instances of DataBlock(s) with the same key for each of the cluster instances, ",
              " and write on each DataBlock with ", numthreads, " threads")) {

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

              auto cluster = std::make_shared<KineticCluster>(
                  "testcluster", blocksize, std::chrono::seconds(10),
                  std::move(connections), ec, repl
              );

              for (int bl = 0; bl < numblocks; bl++) {
                dblocks.push_back(
                    std::unique_ptr<DataBlock>(
                        new DataBlock(cluster, std::make_shared<std::string>("key"), DataBlock::Mode::CREATE))
                );
              }
            }
            REQUIRE(dblocks.size() == (size_t) instances * numblocks);
            sleep(1);

            vector<std::thread> threads;
            for (auto it = dblocks.begin(); it != dblocks.end(); it++) {
              for (int t = 0; t < numthreads; t++) {
                threads.push_back(std::thread(std::bind(write_100, std::ref(*(it->get())))));
              }
            }
            for (size_t t = 0; t < threads.size(); t++) {
              threads[t].join();
            }
          }
        }
      }
    }
  }
}


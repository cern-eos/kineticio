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
#include "DataBlock.hh"
#include "KineticCluster.hh"
#include "SimulatorController.h"
#include "catch.hpp"

using namespace kio;

SCENARIO("DataBlock integration test.", "[Data]")
{
  auto& c = SimulatorController::getInstance();
  REQUIRE(c.reset(0));

  SocketListener listener;
  int nData = 1;
  int nParity = 0;
  int blocksize = 1024 * 1024;

  std::vector<std::unique_ptr<KineticAutoConnection>> connections;
  std::unique_ptr<KineticAutoConnection> autocon(
      new KineticAutoConnection(listener, std::make_pair(c.get(0), c.get(0)), std::chrono::seconds(10))
  );
  connections.push_back(std::move(autocon));


  auto cluster = std::make_shared<KineticCluster>("testcluster", blocksize, std::chrono::seconds(10),
                                                  std::move(connections),
                                                  std::make_shared<RedundancyProvider>(nData, nParity),
                                                  std::make_shared<RedundancyProvider>(nData, nParity)
  );

  GIVEN ("An empty block with create flag set.") {
    DataBlock data(cluster, std::make_shared<std::string>("key"), DataBlock::Mode::CREATE);

    THEN("Illegal writes fail.") {
      char buf[10];
      /* nullpointer */
      REQUIRE_THROWS_AS(data.write(NULL, 0, 0), std::system_error);
      /* writing past block size limit. */
      REQUIRE_THROWS_AS(data.write(buf, cluster->limits(KeyType::Data).max_value_size + 1, 1), std::system_error);
    }

    THEN("The data is dirty.") {
      REQUIRE(data.dirty());
    }

    WHEN("The empty block is flushed.") {
      REQUIRE_NOTHROW(data.flush());
      THEN("The block is not dirty.") {
        REQUIRE_FALSE(data.dirty());
      }
    }

    WHEN("Something is written to the block.") {
      char in[] = "0123456789";
      REQUIRE_NOTHROW(data.write(in, 0, sizeof(in)));

      THEN("It is dirty") {
        REQUIRE(data.dirty());
      }

      THEN("It can be read again from memory.") {
        char out[sizeof(in)];
        REQUIRE_NOTHROW(data.read(out, 0, sizeof(out)));
        REQUIRE((memcmp(in, out, sizeof(in)) == 0));
      }

      AND_WHEN("It is truncated to size 0.") {
        REQUIRE_NOTHROW(data.truncate(0));
        REQUIRE((data.size() == 0));

        THEN("Reading from the block returns 0s.") {
          char out[] = "0123456789";
          char compare[10];
          memset(compare, 0, 10);

          REQUIRE_NOTHROW(data.read(out, 0, 10));
          REQUIRE((memcmp(compare, out, 10) == 0));
        }
      }

      AND_WHEN("It is flushed.") {
        REQUIRE_NOTHROW(data.flush());

        THEN("It can be read again from the drive.") {
          DataBlock x(cluster, std::make_shared<std::string>("key"));
          char out[10];
          REQUIRE_NOTHROW(x.read(out, 0, 10));
          REQUIRE((memcmp(in, out, 10) == 0));
        }

        THEN("It is no longer dirty.") {
          REQUIRE_FALSE(data.dirty());
        }

        AND_WHEN("The on-drive value is manipulated by someone else.") {
          DataBlock x(cluster, std::make_shared<std::string>("key"));
          REQUIRE_NOTHROW(x.write("99", 0, 2));
          REQUIRE_NOTHROW(x.flush());

          THEN("The change is not visible immediately.") {
            char out[10];
            REQUIRE_NOTHROW(data.read(out, 0, 10));
            REQUIRE((memcmp(in, out, 10) == 0));

            AND_THEN("It will become visible after expiration time has run out.") {
              usleep(data.expiration_time.count() * 1000);
              REQUIRE_NOTHROW(data.read(out, 0, 10));
              REQUIRE((memcmp(in, out, 10) != 0));
            }
          }
        }
      }
    }
  }
}

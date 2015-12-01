#include <unistd.h>
#include "catch.hpp"
#include "DataBlock.hh"
#include "KineticCluster.hh"
#include "SimulatorController.h"

using namespace kio;

SCENARIO("Chunk integration test.", "[Chunk]"){
  auto& c = SimulatorController::getInstance();
  c.start(0);
  REQUIRE( c.reset(0) );

  SocketListener listener;
  std::vector< std::pair < kinetic::ConnectionOptions, kinetic::ConnectionOptions > > info;
  info.push_back(std::pair<kinetic::ConnectionOptions,kinetic::ConnectionOptions>(c.get(0),c.get(0)));
  int nData = 1;
  int nParity = 0;
  int blocksize = 1024*1024;
  auto cluster = std::make_shared<KineticCluster>("testcluster", nData, nParity, blocksize, info,
                                                  std::chrono::seconds(20),
                                                  std::chrono::seconds(10),
                                                  std::make_shared<RedundancyProvider>(nData,nParity),
                                                  listener
  );

  GIVEN ("An empty block with create flag set."){
    DataBlock data(cluster, std::make_shared<std::string>("key"), DataBlock::Mode::CREATE);

    THEN("Illegal writes fail."){
      char buf[10];
      /* nullpointer */
      REQUIRE_THROWS_AS(data.write(NULL, 0, 0), std::invalid_argument);
      /* writing past block size limit. */
      REQUIRE_THROWS_AS(data.write(buf, cluster->limits().max_value_size+1, 1), std::invalid_argument);
    }

    THEN("The data is dirty."){
      REQUIRE(data.dirty());
    }

    WHEN("The empty block is flushed."){
      REQUIRE_NOTHROW(data.flush());
      THEN("The block is not dirty."){
        REQUIRE_FALSE(data.dirty());
      }
    }

    WHEN("Something is written to the block."){
      char in[] = "0123456789";
      REQUIRE_NOTHROW(data.write(in, 0, sizeof(in)));

      THEN("It is dirty"){
        REQUIRE(data.dirty());
      }

      THEN("It can be read again from memory."){
        char out[sizeof(in)];
        REQUIRE_NOTHROW(data.read(out,0,sizeof(out)));
        REQUIRE(memcmp(in,out,sizeof(in)) == 0);
      }

      AND_WHEN("It is truncated to size 0."){
        REQUIRE_NOTHROW(data.truncate(0));
        REQUIRE(data.size() == 0);

        THEN("Reading from the block returns 0s."){
          char out[] = "0123456789";
          char compare[10];
          memset(compare,0,10);

          REQUIRE(cluster->limits().max_value_size > 0);
          REQUIRE_NOTHROW(data.read(out,0,10));
          REQUIRE(memcmp(compare,out,10) == 0);
        }
      }

      AND_WHEN("It is flushed."){
        REQUIRE_NOTHROW(data.flush());

        THEN("It can be read again from the drive."){
          DataBlock x(cluster, std::make_shared<std::string>("key"));
          char out[10];
          REQUIRE_NOTHROW(x.read(out,0,10));
          REQUIRE(memcmp(in,out,10) == 0);
        }

        THEN("It is no longer dirty."){
          REQUIRE_FALSE(data.dirty());
        }

        AND_WHEN("The on-drive value is manipulated by someone else."){
          DataBlock x(cluster, std::make_shared<std::string>("key"));
          REQUIRE_NOTHROW(x.write("99",0,2));
          REQUIRE_NOTHROW(x.flush());

          THEN("The change is not visible immediately."){
            char out[10];
            REQUIRE_NOTHROW(data.read(out,0,10));
            REQUIRE(memcmp(in,out,10) == 0);

            AND_THEN("It will become visible after expiration time has run out."){
              usleep(data.expiration_time.count() * 1000);
              REQUIRE_NOTHROW(data.read(out,0,10));
              REQUIRE(memcmp(in,out,10) != 0);
            }
          }
        }
      }
    }
  }
}

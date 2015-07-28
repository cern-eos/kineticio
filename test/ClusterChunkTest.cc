#include <errno.h>
#include <unistd.h>
#include "catch.hpp"
#include "ClusterChunk.hh"
#include "KineticCluster.hh"
#include "SimulatorController.h"

using namespace kio;

SCENARIO("Chunk integration test.", "[Chunk]"){
  auto& c = SimulatorController::getInstance();
  c.start(0);
  REQUIRE( c.reset(0) );

  GIVEN ("An empty chunk."){
    int nData = 1;
    int nParity = 0;
    
    SocketListener listener;
    std::vector< std::pair < kinetic::ConnectionOptions, kinetic::ConnectionOptions > > info;
    info.push_back(std::pair<kinetic::ConnectionOptions,kinetic::ConnectionOptions>(c.get(0),c.get(0)));
    auto cluster = std::make_shared<KineticCluster>(nData, nParity, info,
            std::chrono::seconds(20),
            std::chrono::seconds(10),
            std::make_shared<ErasureCoding>(nData,nParity),
            listener
    );

    ClusterChunk c(cluster, std::make_shared<std::string>("key"), ClusterChunk::Mode::CREATE);

    THEN("Illegal writes to the chunk fail."){
      char buf[10];
      /* nullpointer */
      REQUIRE_THROWS(c.write(NULL, 0, 0));
      /* writing past chunk size limit. */
      REQUIRE_THROWS(c.write(buf, cluster->limits().max_value_size+1, 1));
    }

    THEN("The chunk is dirty."){
      REQUIRE(c.dirty() == true);
    }

    WHEN("The empty chunk is flushed."){
      REQUIRE_NOTHROW(c.flush());
      THEN("The chunk is not dirty."){
        REQUIRE(c.dirty() == false);
      }
    }

    WHEN("Something is written to the chunk."){
      char in[] = "0123456789";
      REQUIRE_NOTHROW(c.write(in, 0, sizeof(in)));

      THEN("It can be read again from memory."){
        char out[sizeof(in)];
        REQUIRE_NOTHROW(c.read(out,0,sizeof(out)));
        REQUIRE(memcmp(in,out,sizeof(in)) == 0);
      }

      THEN("It is dirty"){
        REQUIRE(c.dirty() == true);
      }

      AND_WHEN("It is truncated to size 0."){
        REQUIRE_NOTHROW(c.truncate(0));
        REQUIRE(c.size() == 0);

        THEN("Reading from the chunk returns 0s."){
          char out[] = "0123456789";
          char compare[10];
          memset(compare,0,10);

          REQUIRE(cluster->limits().max_value_size > 0);
          REQUIRE_NOTHROW(c.read(out,0,10));
          REQUIRE(memcmp(compare,out,10) == 0);
        }
      }

      AND_WHEN("It is flushed."){
        REQUIRE_NOTHROW(c.flush());

        THEN("It can be read again from the drive."){
          ClusterChunk x(cluster, std::make_shared<std::string>("key"));
          char out[10];
          REQUIRE_NOTHROW(x.read(out,0,10));
          REQUIRE(memcmp(in,out,10) == 0);
        }

        THEN("It is no longer dirty."){
          REQUIRE(c.dirty() == false);
        }

        AND_WHEN("The on-drive value is manipulated by someone else."){
          ClusterChunk x(cluster, std::make_shared<std::string>("key"));
          REQUIRE_NOTHROW(x.write("99",0,2));
          REQUIRE_NOTHROW(x.flush());

          THEN("The change is not visible immediately."){
            char out[10];
            REQUIRE_NOTHROW(c.read(out,0,10));
            REQUIRE(memcmp(in,out,10) == 0);

            AND_THEN("It will become visible after expiration time has run out."){
              usleep(c.expiration_time.count() * 1000);
              REQUIRE_NOTHROW(c.read(out,0,10));
              REQUIRE(memcmp(in,out,10) != 0);
            }
          }
        }
      }
    }
  }
}

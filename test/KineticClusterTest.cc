#include <unistd.h>
#include "catch.hpp"
#include "KineticCluster.hh"
#include "SimulatorController.h"

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

    auto cluster = make_shared<KineticCluster>(nData, nParity, info,
                                               std::chrono::seconds(20),
                                               std::chrono::seconds(2),
                                               std::make_shared<ErasureCoding>(nData, nParity),
                                               listener
    );

    THEN("cluster limits reflect kinetic-drive limits") {
      REQUIRE(cluster->limits().max_key_size == 4096);
      REQUIRE(cluster->limits().max_value_size == nData * 1024 * 1024);
    }

    THEN("Cluster size is reported as long as a single drive is alive.") {

      ClusterSize s;
      REQUIRE(cluster->size(s).ok());
      // sleep 500 ms to let cluster size update in bg
      usleep(1000 * 500);

      for (int i = 0; i < nData + nParity + 1; i++) {
        if (i == nData + nParity) {
          auto code = cluster->size(s).statusCode();
          REQUIRE(code == StatusCode::CLIENT_IO_ERROR);
        }
        else {
          REQUIRE(cluster->size(s).ok());
          REQUIRE(s.bytes_total > 0);
          c.stop(i);
        }
      }
    }

    WHEN("Putting a key-value pair") {
      auto value = make_shared<string>(cluster->limits().max_value_size, 'v');

      shared_ptr<const string> putversion;
      auto status = cluster->put(
          make_shared<string>("key"),
          make_shared<string>("version"),
          value,
          true,
          putversion);
      REQUIRE(status.ok());
      REQUIRE(putversion);

      THEN("With == nParity drive failures.") {
        for (int i = 0; i < nParity; i++)
          c.stop(i);

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
      }
    }
  }
}

//
//SCENARIO("Cluster integration test.", "[Cluster]")
//{
//
//  SocketListener listener;
//  GIVEN ("A valid drive cluster") {
//
//
//    auto ember1 = ConnectionOptions{"it-ember1", 8123, false, 1, "ember123"};
//    auto ember2 = ConnectionOptions{"it-ember2", 8123, false, 1, "ember123"};
//    auto ember3 = ConnectionOptions{"it-ember3", 8123, false, 1, "ember123"};
//    auto ember4 = ConnectionOptions{"it-ember4", 8123, false, 1, "ember123"};
//    std::vector<std::pair<ConnectionOptions, ConnectionOptions> > info{
//        std::pair<ConnectionOptions, ConnectionOptions>(ember1, ember1),
//        std::pair<ConnectionOptions, ConnectionOptions>(ember2, ember2),
//        std::pair<ConnectionOptions, ConnectionOptions>(ember3, ember3),
//        std::pair<ConnectionOptions, ConnectionOptions>(ember4, ember4)
//    };
//
//    std::size_t nData = 2;
//    std::size_t nParity = 1;
//    auto cluster = make_shared<KineticCluster>(nData, nParity, info,
//                                               std::chrono::seconds(2),
//                                               std::chrono::seconds(2),
//                                               std::make_shared<ErasureCoding>(nData, nParity),
//                                               listener
//    );
//
//
//    THEN("Cluster size is reported as long as a single drive is alive.") {
//
//      ClusterSize s;
//      REQUIRE(cluster->size(s).ok());
//      // sleep 500 ms to let cluster size update in bg
//      usleep(1000 * 10000);
//      REQUIRE(cluster->size(s).ok());
//      REQUIRE(s.bytes_total > 0);
//    }
//
//    WHEN("Putting a key-value pair") {
//
//      printf("put start\n");
//      auto value = make_shared<string>(cluster->limits().max_value_size, 'v');
//
//      shared_ptr<const string> putversion;
//      auto status = cluster->put(
//          make_shared<string>("key"),
//          make_shared<string>("version"),
//          value,
//          true,
//          putversion);
//      REQUIRE(status.ok());
//      REQUIRE(putversion);
//      printf("put done\n");
//
//      THEN("With == nParity drive failures.") {
//        THEN("Removing it with the correct version succeeds") {
//          auto status = cluster->remove(
//              make_shared<string>("key"),
//              putversion,
//              false);
//          REQUIRE(status.ok());
//        }
//      }
//    }
//  }
//}

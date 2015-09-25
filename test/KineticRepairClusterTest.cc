#include <unistd.h>
#include "catch.hpp"
#include "KineticAdminCluster.hh"
#include "SimulatorController.h"

using std::shared_ptr;
using std::string;
using std::make_shared;
using namespace kinetic;
using namespace kio;

SCENARIO("Repair integration test.", "[Repair]")
{
  auto& c = SimulatorController::getInstance();
  c.start(0);
  c.start(1);
  c.start(2);

  SocketListener listener;

  GIVEN ("A valid repair cluster") {
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

    auto cluster = make_shared<KineticAdminCluster>(nData, nParity, blocksize, info,
                                               std::chrono::seconds(1),
                                               std::chrono::seconds(1),
                                               std::make_shared<ErasureCoding>(nData, nParity, 5),
                                               listener
    );

    WHEN("Putting a key-value pair with one drive down") {
      c.stop(0);

      auto value = make_shared<const string>(cluster->limits().max_value_size, 'v');

      shared_ptr<const string> putversion;
      auto status = cluster->put(
          make_shared<string>("key"),
          make_shared<string>("version"),
          value,
          true,
          putversion);
      REQUIRE(status.ok());
      REQUIRE(putversion);

      THEN("It is marked as incomplete during a scan"){
        auto kc = cluster->scan();
        REQUIRE(kc.total == 1);
        REQUIRE(kc.incomplete == 1);
        REQUIRE(kc.need_repair == 0);
        REQUIRE(kc.removed == 0);
        REQUIRE(kc.repaired == 0);
        REQUIRE(kc.unrepairable == 0);
      }
      THEN("We can't repair it while the drive is down."){
        REQUIRE(cluster->repair().repaired == 0);
      }
      THEN("We can still remove it by resetting the cluster."){
        REQUIRE(cluster->reset().removed == 1);
      }

      AND_WHEN("The drive comes up again."){
        c.start(0);
        cluster->size();
        // wait for connection to reconnect
        sleep(2);

        THEN("It is no longer marked as incomplete but as need_repair after a scan"){
          auto kc = cluster->scan();
          REQUIRE(kc.total == 1);
          REQUIRE(kc.incomplete == 0);
          REQUIRE(kc.need_repair == 1);
          REQUIRE(kc.removed == 0);
          REQUIRE(kc.repaired == 0);
          REQUIRE(kc.unrepairable == 0);
        }
        THEN("We can repair the key.") {
          REQUIRE(cluster->repair().repaired == 1);
        }
        THEN("We can reset the cluster."){
          REQUIRE(cluster->reset().removed == 1);
        }
      }
    }
  }
}

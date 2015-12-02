#include "catch.hpp"
#include "KineticIoSingleton.hh"
#include "KineticIoFactory.hh"
#include "SimulatorController.h"

using namespace kio;

SCENARIO("KineticClusterMap Public API.", "[ClusterMap]"){
  auto& c = SimulatorController::getInstance();
  c.start(0);
  c.start(1);
  c.start(2);
  
  GIVEN("A valid path."){
    auto& kcm = kio::kio().cmap();
    auto r = kio::RedundancyType::REPLICATION;
    THEN("An existing id returns a cluster."){
        REQUIRE_NOTHROW(kcm.getCluster("Cluster1", r));
    }
    THEN("A nonexisting cluster id returns ENODEV."){
        REQUIRE_THROWS(kcm.getCluster("nonExistingID", r));
    }
  }
}

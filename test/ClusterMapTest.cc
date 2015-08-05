#include "catch.hpp"
#include "ClusterMap.hh"

using namespace kio;


SCENARIO("KineticClusterMap Public API.", "[ClusterMap]"){
    GIVEN("A valid path."){
        auto& kcm = ClusterMap::getInstance();

        THEN("Map size equals number of entries."){
            REQUIRE(kcm.getSize() == 3);
        }
        THEN("An existing id to a running device returns a cluster."){
            REQUIRE_NOTHROW(kcm.getCluster("Cluster1"));
        }
        THEN("A nonexisting cluster id returns ENODEV."){
            REQUIRE_THROWS(kcm.getCluster("nonExistingID"));
        }
    }
}

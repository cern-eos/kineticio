#include "catch.hpp"
#include "ClusterMap.hh"
#include <exception>


SCENARIO("KineticClusterMap Public API.", "[ClusterMap]"){
    GIVEN("An invalid path"){
        std::string location(getenv("KINETIC_DRIVE_LOCATION") ? getenv("KINETIC_DRIVE_LOCATION") : "" );
        setenv("KINETIC_DRIVE_LOCATION", "nonExistingFileName", 1);

        ClusterMap kcm;
        THEN("Map is empty"){
            REQUIRE(kcm.getSize() == 0);
        }
        setenv("KINETIC_DRIVE_LOCATION", location.c_str(), 1);
    }

    GIVEN("A path to a non json file"){
        std::string location(getenv("KINETIC_DRIVE_LOCATION") ? getenv("KINETIC_DRIVE_LOCATION") : "" );
        setenv("KINETIC_DRIVE_LOCATION", "kinetic-test", 1);

        ClusterMap kcm;
        THEN("Map is empty"){
            REQUIRE(kcm.getSize() == 0);
        }
        setenv("KINETIC_DRIVE_LOCATION", location.c_str(), 1);
    }

    GIVEN("A valid path."){
        ClusterMap kcm;
        std::shared_ptr<ClusterInterface> c;

        THEN("Map size equals number of entries."){
            REQUIRE(kcm.getSize() == 3);
        }
        THEN("An existing id to a running device returns a working cluster."){
            REQUIRE_NOTHROW(kcm.getCluster("Cluster1"));
        }
        THEN("A nonexisting drive id returns ENODEV."){
            REQUIRE_THROWS(kcm.getCluster("nonExistingID"));
        }
    }
}

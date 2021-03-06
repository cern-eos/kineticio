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

#include "KineticIoSingleton.hh"
#include "catch.hpp"

using namespace kio;

SCENARIO("KineticClusterMap Public API.", "[ClusterMap]"){
  GIVEN("A valid path."){
    auto& kcm = kio::kio().cmap();
    THEN("Existing cluster ids return a cluster."){
        REQUIRE_NOTHROW(kcm.getCluster("Cluster1"));
        REQUIRE_NOTHROW(kcm.getCluster("Cluster2"));
        REQUIRE_NOTHROW(kcm.getCluster("Cluster3"));
    }
    THEN("A nonexisting cluster id returns ENODEV."){
        REQUIRE_THROWS(kcm.getCluster("nonExistingID"));
    }
  }
}

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

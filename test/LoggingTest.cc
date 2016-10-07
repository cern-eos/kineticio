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

#include "Logging.hh"
#include "catch.hpp"

using namespace kio;

SCENARIO("LoggingTest", "[log]"){

  GIVEN (""){
    THEN("We can log arbitrary length arbitrary type chains"){
      int i = 1;
      double d = 0.99;
      std::string s = "'happy'";
      kinetic::KineticStatus status (kinetic::StatusCode::OK, "Test message.");
      kio_notice("Logging Test: Integer ", i,", Double ", d, ", String ", s, ", KineticStatus ", status);
    }
  }
};
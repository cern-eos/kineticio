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

#include <unistd.h>
#include "catch.hpp"
#include "KineticAutoConnection.hh"
#include "SimulatorController.h"

using std::shared_ptr;
using std::string;
using std::make_shared;
using namespace kinetic;
using namespace kio;


namespace {
class ConnectCallback : public kinetic::SimpleCallbackInterface {
public:
  void Success()
  { _done = _success = true; }

  void Failure(kinetic::KineticStatus error)
  { _done = true; }

  ConnectCallback() : _done(false), _success(false)
  { }

  ~ConnectCallback()
  { }

  bool done()
  { return _done; }

  bool ok()
  { return _success; }

private:
  bool _done;
  bool _success;
};
}


SCENARIO("Connection Test", "[Con]")
{
  auto& c = SimulatorController::getInstance();
  c.enable(0);

  SocketListener listener;
  fd_set x;
  int y;

  GIVEN ("An autoconnection") {

    auto info = std::make_pair(c.get(0), c.get(0));
    auto autocon = std::make_shared<KineticAutoConnection>(listener, info, std::chrono::seconds(1));

    THEN("It is accessible.") {
      auto con = autocon->get();
      auto cb = std::make_shared<ConnectCallback>();
      con->NoOp(cb);
      con->Run(&x, &x, &y);
      usleep(1000 * 100);
      REQUIRE(cb->done());
      REQUIRE(cb->ok());

      AND_WHEN("We set it into error state with wrong connection pointer"){
        std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection> empty;
        autocon->setError(empty);
        THEN("it will have no effect."){
          auto con = autocon->get();
          auto cb = std::make_shared<ConnectCallback>();
          con->NoOp(cb);
          con->Run(&x, &x, &y);
          usleep(1000 * 100);
          REQUIRE(cb->done());
          REQUIRE(cb->ok());
        }
      }

      AND_THEN("When we set it into error state by hand. ") {
        autocon->setError(con);
        usleep(1000 * 2000);

        AND_THEN("it will reconnect.") {
          REQUIRE_THROWS(autocon->get());
          usleep(1000 * 1000);

          AND_THEN("be usable") {
            auto con = autocon->get();
            auto cb = std::make_shared<ConnectCallback>();
            con->NoOp(cb);
            con->Run(&x, &x, &y);
            usleep(1000 * 100);
            REQUIRE(cb->done());
            REQUIRE(cb->ok());
          }
        }
      }


      AND_THEN("When we kill the simulator, we can still get the connection, but noop will not succeed.") {
        c.block(0);
        auto con = autocon->get();
        auto cb = std::make_shared<ConnectCallback>();
        con->NoOp(cb);
        con->Run(&x, &x, &y);
        usleep(1000 * 100);
        REQUIRE(cb->done());
        REQUIRE(!cb->ok());
      }
    }

  }
}

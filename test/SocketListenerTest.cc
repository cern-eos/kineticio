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
#include "KineticAutoConnection.hh"
#include "SimulatorController.h"

using std::shared_ptr;
using std::string;
using std::make_shared;

using namespace kio;
using namespace kinetic;


class AnotherSimpleCallback : public kinetic::SimpleCallbackInterface {
public:
    void Success() {
      rdy=true;
      cv.notify_one();
    }

    void Failure(kinetic::KineticStatus error) {
      rdy=true;
      cv.notify_one();
    }

    AnotherSimpleCallback(std::condition_variable& cv, bool& rdy) :
    cv(cv),rdy(rdy){};

private:
    std::condition_variable& cv;
    bool & rdy;
};

SCENARIO("Listener Test.", "[Listen]"){

  auto& c = SimulatorController::getInstance();
  c.start(0);
  REQUIRE( c.reset(0) );

  GIVEN ("stuff"){
    kio::SocketListener listen;
    kio::KineticAutoConnection con(
      listen,
      std::pair<ConnectionOptions,ConnectionOptions>(c.get(0),c.get(0)),
      std::chrono::seconds(10));

    THEN(" We can register the connection"){

      AND_THEN("Callback will be automatically called."){
        std::condition_variable cv;
        bool ready;
        auto cb = make_shared<AnotherSimpleCallback>(cv, ready);

        con.get()->NoOp(cb);
        fd_set a; int fd;
        con.get()->Run(&a,&a,&fd);

        std::mutex mtx;
        std::unique_lock<std::mutex> lck(mtx);
        while (!ready) cv.wait(lck);
        
      }
    }
  }
};
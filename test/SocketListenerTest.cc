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
      std::lock_guard<std::mutex> lock(mutex);
      rdy=true;
      cv.notify_one();
    }

    void Failure(kinetic::KineticStatus error) {
      std::lock_guard<std::mutex> lock(mutex);
      rdy=true;
      cv.notify_one();
    }

    AnotherSimpleCallback(std::condition_variable& cv, std::mutex& mutex, bool& rdy) :
    cv(cv), mutex(mutex), rdy(rdy) {};

private:
    std::condition_variable& cv;
    std::mutex& mutex;
    bool& rdy;
};

SCENARIO("Listener Test.", "[Listen]"){

  auto& c = SimulatorController::getInstance();
  REQUIRE( c.reset(0) );

  GIVEN ("A Socket Listener"){
    kio::SocketListener listen;

    THEN("We can create a connection that will register with the listener"){
      kio::KineticAutoConnection con(
        listen,
        std::pair<ConnectionOptions,ConnectionOptions>(c.get(0),c.get(0)),
        std::chrono::seconds(10)
      );
      REQUIRE_NOTHROW(con.get());

      AND_THEN("Callbacks will be automatically be called."){
        std::condition_variable cv;
        std::mutex mtx;
        bool ready = false;
        auto cb = make_shared<AnotherSimpleCallback>(cv, mtx, ready);

        con.get()->NoOp(cb);
        fd_set a; int fd;
        con.get()->Run(&a,&a,&fd);

        std::chrono::system_clock::time_point timeout_time = std::chrono::system_clock::now() + std::chrono::seconds(10);
        std::unique_lock<std::mutex> lck(mtx);
        while (!ready && std::chrono::system_clock::now() < timeout_time) {
          cv.wait_until(lck, timeout_time);
        }
        REQUIRE(ready);
      }
    }
  }
};
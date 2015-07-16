#include "catch.hpp"
#include "KineticAutoConnection.hh"
#include "SocketListener.hh"
#include <unistd.h>
#include <condition_variable>
#include <chrono>

using std::shared_ptr;
using std::string;
using std::make_shared;

using namespace kio;
using namespace kinetic;


class AnotherSimpleCallback : public kinetic::SimpleCallbackInterface {
public:
    void Success() {
      printf("test callback success\n");
      rdy=true;
      cv.notify_one();
    }

    void Failure(kinetic::KineticStatus error) {
      printf("test callback failure\n");
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

  ConnectionOptions target1 =  { "localhost", 8123, false, 1, "asdfasdf" };
  ConnectionOptions tls_t1 =   { "localhost", 8443, true, 1, "asdfasdf" };
  ConnectionOptions target2 =  { "localhost", 8124, false, 1, "asdfasdf" };
  ConnectionOptions tls_t2 =   { "localhost", 8444, true, 1, "asdfasdf" };

  kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
  std::shared_ptr<kinetic::BlockingKineticConnection> con1;
  std::shared_ptr<kinetic::BlockingKineticConnection> con2;
  REQUIRE(factory.NewBlockingConnection(tls_t1, con1, 30).ok());
  REQUIRE(factory.NewBlockingConnection(tls_t2, con2, 30).ok());
  REQUIRE(con1->InstantErase("NULL").ok());
  REQUIRE(con2->InstantErase("NULL").ok());

  GIVEN ("stuff"){
    kio::SocketListener listen;
    kio::KineticAutoConnection con(
      listen,
      std::pair<ConnectionOptions,ConnectionOptions>(target1,target1),
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
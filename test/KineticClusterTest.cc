#include "catch.hpp"
#include "KineticCluster.hh"

using std::shared_ptr;
using std::string;
using std::make_shared;
using namespace kinetic;

SCENARIO("Cluster integration test.", "[Cluster]"){

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

  std::vector< std::pair<ConnectionOptions,ConnectionOptions> > info;
    info.push_back(std::pair<ConnectionOptions,ConnectionOptions>(target1,target1));
    info.push_back(std::pair<ConnectionOptions,ConnectionOptions>(target2,target2));
  
  GIVEN ("A drive cluster"){


    auto cluster = make_shared<KineticCluster>(1, 1, info, 
            std::chrono::seconds(20),
            std::chrono::seconds(10)
    );

    THEN("cluster limits reflect kinetic-drive limits"){
      REQUIRE(cluster->limits().max_key_size == 4096);
      REQUIRE(cluster->limits().max_value_size == 1024*1024);
    }

    THEN("Cluster size is reported."){
      KineticClusterSize s = {0,0};
      REQUIRE(cluster->size(s).ok());
      REQUIRE(s.bytes_free > 0);
      REQUIRE(s.bytes_total > 0);
    }

    WHEN("Putting a key-value pair"){
      shared_ptr<const string> newversion;
      auto status = cluster->put(
          make_shared<string>("key"),
          make_shared<string>("version"),
          make_shared<string>("value"),
          true,
          newversion);
      REQUIRE(status.ok());
      REQUIRE(newversion);

      THEN("It can be read in again."){
        shared_ptr<const string> version;
        shared_ptr<const string> value;
        auto status = cluster->get(
          make_shared<string>("key"),
          false,
          version,
          value);
        printf("%s\n",status.message().c_str());
        REQUIRE(status.statusCode() == StatusCode::OK);
        REQUIRE(version);
        REQUIRE(version->compare(*newversion)==0);
        REQUIRE(value);
        REQUIRE(value->compare("value")==0);
      }

      THEN("Removing it with an incorrect version fails with INVALID_VERSION Status Code"){
        auto status = cluster->remove(
          make_shared<string>("key"),
          make_shared<string>("incorrect"),
          false);
          REQUIRE(status.statusCode() == kinetic::StatusCode::REMOTE_VERSION_MISMATCH);
      }

      THEN("It can be removed again."){
        auto status = cluster->remove(
          make_shared<string>("key"),
          make_shared<string>(),
          true);
        REQUIRE(status.ok());
      }
    }
  }
};
#include "catch.hpp"
#include "FileIo.hh"
#include "DataCache.hh"
#include <kio/FileIoInterface.hh>
#include "ClusterInterface.hh"
#include "Utility.hh"
#include <chrono>
#include <unistd.h>


using namespace kio;
using namespace kinetic;
using namespace std::chrono;

class MockCluster : public ClusterInterface {
public:  
  const std::string& instanceId() const
  {
    return _id;
  }

  const std::string& id() const
  {
    return _id;
  }
  
  const ClusterLimits& limits() const
  {
      return _limits; 
  };

  ClusterSize size()
  {
      return _size; 
  };
  
  ClusterIo iostats()
  {
    return {0,0,0,0,0};
  }
  
  kinetic::KineticStatus get(
    const std::shared_ptr<const std::string>& key,
    bool skip_value,
    std::shared_ptr<const std::string>& version,
    std::shared_ptr<const std::string>& value)
  {
      version = utility::uuidGenerateEncodeSize(128);
      value = std::make_shared<const string>('x',128);
      return KineticStatus(StatusCode::OK, "");
  };

  kinetic::KineticStatus put(
    const std::shared_ptr<const std::string>& key,
    const std::shared_ptr<const std::string>& version,
    const std::shared_ptr<const std::string>& value,
    bool force,
    std::shared_ptr<const std::string>& version_out)
  {
    version_out = utility::uuidGenerateEncodeSize(128);
    return KineticStatus(StatusCode::OK, "");
  };

  kinetic::KineticStatus remove(
    const std::shared_ptr<const std::string>& key,
    const std::shared_ptr<const std::string>& version,
    bool force)
  {
    return KineticStatus(StatusCode::OK, "");
  };

  kinetic::KineticStatus range(
    const std::shared_ptr<const std::string>& start_key,
    const std::shared_ptr<const std::string>& end_key,
    int maxRequested,
    std::unique_ptr< std::vector<std::string> >& keys)
  {
    return KineticStatus(StatusCode::OK, ""); 
  };

  MockCluster() : _limits{128,128,128}, _size{128,128} { }
  ~MockCluster(){};
  
private:
    kio::ClusterLimits _limits;
    kio::ClusterSize _size;
    std::string _id;
};

class MockFileIo : public kio::FileIo {
public:

  MockFileIo(std::string path, std::shared_ptr<kio::ClusterInterface> c) {
    cluster = c; 
    path = path; 
    }
  ~MockFileIo (){};

};

SCENARIO("Cache Performance Test.", "[Cache]"){ 
    
    GIVEN("A Cache Object and a mocked FileIo object"){
      
      THEN("go ~"){
      for(int capacity = 1000; capacity < 100000; capacity*=5){
      
        DataCache ccc(capacity*128, 20, 20, 0);
        std::shared_ptr<ClusterInterface> cluster(new MockCluster());
        MockFileIo fio("thepath",cluster);

        printf("Cache get() performance for a cache with capacity of %d items \n", capacity);             

        int break_point = capacity*0.7;
        auto tstart = system_clock::now();
        for(int i=0; i<break_point; i++)
            ccc.get((FileIo*)&fio, i, DataBlock::Mode::STANDARD, false);
        auto tend = system_clock::now();

        printf("%ld items per second up to 70 percent capacity\n", (capacity * 700) / (duration_cast<milliseconds>(tend-tstart).count()+1));

        tstart = system_clock::now();
        for(int i=break_point; i<capacity; i++)
          ccc.get((FileIo*)&fio, i, DataBlock::Mode::STANDARD, false);
        tend = system_clock::now();
        printf("%ld items per second up to capacity \n", (capacity * 300) / (duration_cast<milliseconds>(tend-tstart).count()+1));

        tstart = system_clock::now();
        for(int i=capacity; i<2*capacity; i++)
          ccc.get((FileIo*)&fio, i, DataBlock::Mode::STANDARD, false);
        tend = system_clock::now();
        printf("%ld items per second above capacity \n", (capacity * 1000) / (duration_cast<milliseconds>(tend-tstart).count()+1));

        printf("Waiting for cache items to time out so they qualify for removal\n");
        sleep(6);

        tstart = system_clock::now();
        for(int i=2*capacity; i<3*capacity; i++)
          ccc.get((FileIo*)&fio, i, DataBlock::Mode::STANDARD, false);
        tend = system_clock::now();
        printf("%ld items per second above capacity after timeout \n\n", (capacity * 1000) / (duration_cast<milliseconds>(tend-tstart).count()+1));
      }
      }
      
    }
}


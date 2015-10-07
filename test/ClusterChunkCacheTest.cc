#include "catch.hpp"
#include "FileIo.hh"
#include "ClusterChunkCache.hh"
#include <kio/FileIoInterface.hh>
#include "ClusterInterface.hh"
#include "Utility.hh"
#include <chrono>


using namespace kio;
using namespace kinetic;
using namespace std::chrono;

class MockCluster : public ClusterInterface {
public:  
  const ClusterLimits& limits() const
  {
      return _limits; 
  };

  ClusterSize size()
  {
      return _size; 
  };

  kinetic::KineticStatus get(
    const std::shared_ptr<const std::string>& key,
    bool skip_value,
    std::shared_ptr<const std::string>& version,
    std::shared_ptr<const std::string>& value)
  {
      version = utility::uuidGenerateEncodeSize(1024);
      value = std::make_shared<const string>('x',1024);
      return KineticStatus(StatusCode::OK, "");
  };

  kinetic::KineticStatus put(
    const std::shared_ptr<const std::string>& key,
    const std::shared_ptr<const std::string>& version,
    const std::shared_ptr<const std::string>& value,
    bool force,
    std::shared_ptr<const std::string>& version_out)
  {
    version_out = utility::uuidGenerateEncodeSize(1024);
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

  MockCluster() : _limits{1024,1024,1024}, _size{1024,1024} { }
  ~MockCluster(){};
  
private:
    kio::ClusterLimits _limits;
    kio::ClusterSize _size;
};

class MockFileIo : public kio::FileIo {
public:

  MockFileIo(std::string path, std::shared_ptr<kio::ClusterInterface> c){
    cluster = c; 
    chunk_basename = path; 
    }
  ~MockFileIo (){};

};

SCENARIO("Cache Performance Test.", "[Cache]"){ 
    
    GIVEN("A Cache Object"){
       ClusterChunkCache ccc(1024*10000, 1024*20000, 20, 20, 0);
       std::shared_ptr<ClusterInterface> cluster(new MockCluster());
       MockFileIo fio("thepath",cluster);
       
       int num_items = 100000;
       
       auto tstart = system_clock::now();
       for(int i=0; i<num_items; i++){
           ccc.get((FileIo*)&fio, i, ClusterChunk::Mode::STANDARD);
       }
       auto tend = system_clock::now();
             
       printf("%ld items per second\n", (num_items * 1000) / duration_cast<milliseconds>(tend-tstart).count());
       
       
        
    }
}


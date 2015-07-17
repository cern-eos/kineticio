#include "catch.hpp"
#include "Utility.hh"
#include <functional>
#include <thread>
#include <chrono>
#include <vector>
#include <memory>
#include <string>
#include <zlib.h>
#include <fcntl.h>


using namespace kio; 
using std::string;

void po (const std::shared_ptr<const string>& in, uLong& out){
  out = crc32(0, (const Bytef*) in->c_str(), in->length());
}

SCENARIO("Utility Test.", "[Utility]"){

  GIVEN ("A size attribute"){
    size_t target_size = 112323;

    WHEN("We encode a size attribute in version Information. "){
      auto v = utility::uuidGenerateEncodeSize(target_size);

      THEN("We can extract the encoded size attribute again."){
        auto extracted_size = utility::uuidDecodeSize(v);
        REQUIRE(target_size == extracted_size);
      }

      WHEN("We manipulate the version size"){
        auto v2 = std::make_shared<const std::string>(*v + "123");
        THEN("Trying to extra the size attribute fails. "){
          REQUIRE_THROWS(utility::uuidDecodeSize(v2));
        }
      }
    }
  }

//  GIVEN("a stripe vector"){
//    std::vector< std::shared_ptr<const string> > stripe;
//    int size = 1024*1024;
//    char abuf[size];
//
//    int fd = open("/dev/urandom", O_RDONLY);
//    for(int i=0; i<100; i++){
//    read(fd, abuf, size);
//      stripe.push_back(std::make_shared<const string>((char*)abuf,size));
//    }
//    close(fd);
//    std::vector< uLong > crcs(stripe.size());
//
//    THEN("We can compute crc32 sequentially"){
//
//      std::chrono::system_clock::time_point t = std::chrono::system_clock::now(); 
//      for(int i=0; i<stripe.size(); i++){
//        crcs[i] = crc32(0, (const Bytef*) stripe[i]->c_str(), stripe[i]->length());
//      }
//      printf("creating %ld checksums sequentially took %ld milliseconds\n",
//              crcs.size(),
//              std::chrono::duration_cast<std::chrono::milliseconds>
//                (std::chrono::system_clock::now()-t).count()
//              );
//    }
//    
//    THEN("We can compute crc32 in parallel."){
//      std::chrono::system_clock::time_point t = std::chrono::system_clock::now(); 
//      std::vector<std::thread> threads; 
//      for(int i=0; i<stripe.size(); i++)
//        threads.push_back(std::thread(po, std::cref(stripe[i]), std::ref(crcs[i])));
//      for(int i=0; i<threads.size(); i++)
//        threads[i].join();
//      
//      printf("creating %ld checksums in parallel took %ld milliseconds\n",
//              crcs.size(),
//              std::chrono::duration_cast<std::chrono::milliseconds>
//                (std::chrono::system_clock::now()-t).count()
//              );
//    }
//    
//  }
};
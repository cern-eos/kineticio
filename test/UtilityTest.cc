#include "catch.hpp"
#include "Utility.hh"
#include <fcntl.h>
#include <unistd.h>
#include <zlib.h>

using namespace kio; 
using std::string;

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
  
  GIVEN("a key"){
    auto key = std::make_shared<const string>("/this/is/a/key");
    THEN("We can construct an indicator key"){
      auto indicator = utility::keyToIndicator(*key);
      
      AND_THEN("We can reconstruct the key from the indicator"){
        auto reconstructed = utility::indicatorToKey(*indicator);
        REQUIRE(*reconstructed == *key);
      }
    }
  }

  GIVEN("a stripe vector"){
    std::vector< std::shared_ptr<const string> > stripe;
    int size = 1024*1024;
    char abuf[size];

    int fd = open("/dev/urandom", O_RDONLY);
    for(int i=0; i<100; i++){
    read(fd, abuf, size);
      stripe.push_back(std::make_shared<const string>((char*)abuf,size));
    }
    close(fd);

    THEN("We can compute crc32 and crc32c"){
      std::vector< uint32_t > crcs(stripe.size());

      std::chrono::system_clock::time_point t = std::chrono::system_clock::now();
      for(int i=0; i<stripe.size(); i++){
        crcs[i] = crc32(0, (const Bytef*)stripe[i]->c_str(), stripe[i]->length());
      }
      printf("creating %ld crc32 checksums took %ld milliseconds\n",
             crcs.size(),
             std::chrono::duration_cast<std::chrono::milliseconds>
                 (std::chrono::system_clock::now()-t).count()
      );

      t = std::chrono::system_clock::now();
      for(int i=0; i<stripe.size(); i++){
        crcs[i] = crc32c(0, stripe[i]->c_str(), stripe[i]->length());
      }
      printf("creating %ld crc32c checksums took %ld milliseconds\n",
             crcs.size(),
              std::chrono::duration_cast<std::chrono::milliseconds>
                (std::chrono::system_clock::now()-t).count()
              );
    }
  }
};
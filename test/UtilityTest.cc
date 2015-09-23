#include "catch.hpp"
#include "Utility.hh"
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <zlib.h>

using namespace kio; 
using std::string;

void funcrc32c (const std::shared_ptr<const string>& in, uint32_t& out){
  out = crc32c(0, in->c_str(), in->length());
}
void funcrc32 (const std::shared_ptr<const string>& in, uint32_t& out){
  out = crc32(0, (const Bytef*)in->c_str(), in->length());
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
      std::vector< uint32_t > crcs_a(stripe.size());
      std::vector< uint32_t > crcs_b(stripe.size());

      std::chrono::system_clock::time_point t = std::chrono::system_clock::now();
      for(int i=0; i<stripe.size(); i++){
        crcs_a[i] = crc32(0, (const Bytef*)stripe[i]->c_str(), stripe[i]->length());
      }
      printf("creating %ld crc32 checksums sequentially took %ld milliseconds\n",
             crcs_a.size(),
             std::chrono::duration_cast<std::chrono::milliseconds>
                 (std::chrono::system_clock::now()-t).count()
      );

      t = std::chrono::system_clock::now();
      std::vector<std::thread> threads;
      for(int i=0; i<stripe.size(); i++)
        threads.push_back(std::thread(funcrc32, std::cref(stripe[i]), std::ref(crcs_b[i])));
      for(int i=0; i<threads.size(); i++)
        threads[i].join();

      printf("creating %ld crc32 checksums in parallel took %ld milliseconds\n",
             crcs_b.size(),
             std::chrono::duration_cast<std::chrono::milliseconds>
                 (std::chrono::system_clock::now()-t).count()
      );

      for(int i=0; i<stripe.size(); i++)
        REQUIRE(crcs_a[i] == crcs_b[i]);


      t = std::chrono::system_clock::now();
      for(int i=0; i<stripe.size(); i++){
        crcs_a[i] = crc32c(0, stripe[i]->c_str(), stripe[i]->length());
      }
      printf("creating %ld crc32c checksums sequentially took %ld milliseconds\n",
             crcs_a.size(),
              std::chrono::duration_cast<std::chrono::milliseconds>
                (std::chrono::system_clock::now()-t).count()
              );

      t = std::chrono::system_clock::now();
      threads.resize(0);
      for(int i=0; i<stripe.size(); i++)
        threads.push_back(std::thread(funcrc32c, std::cref(stripe[i]), std::ref(crcs_b[i])));
      for(int i=0; i<threads.size(); i++)
        threads[i].join();

      printf("creating %ld crc32c checksums in parallel took %ld milliseconds\n",
             crcs_b.size(),
             std::chrono::duration_cast<std::chrono::milliseconds>
                 (std::chrono::system_clock::now()-t).count()
      );

      for(int i=0; i<stripe.size(); i++)
        REQUIRE(crcs_a[i] == crcs_b[i]);
    }
  }
};
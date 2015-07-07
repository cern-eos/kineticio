#include "catch.hpp"
#include "Utility.hh"

using namespace kio; 

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
};
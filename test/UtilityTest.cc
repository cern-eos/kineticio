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
#include "Utility.hh"
#include <fcntl.h>
#include <unistd.h>
#include <zlib.h>
#include <Logging.hh>
#include <uuid.h>

using namespace kio; 
using std::string;

SCENARIO("Utility Test.", "[Utility]"){

  GIVEN ("A size attribute"){
    size_t target_size = 112323;

    THEN("uuid decode accepts deprecated binary uuid encoding for backwards compability (eosgenome)"){
      std::ostringstream ss;
      ss << std::setw(10) << std::setfill('0') << target_size;
      uuid_t uuid;
      uuid_generate(uuid);
      auto selfconstructedversion = std::make_shared<const string>(
          ss.str() + utility::Convert::toString(uuid)
      );

      REQUIRE(selfconstructedversion->size() == 26);

      auto extracted_size = utility::uuidDecodeSize(selfconstructedversion);
      REQUIRE((target_size == extracted_size));
    }

    WHEN("We encode a size attribute in version Information. "){
      auto v = utility::uuidGenerateEncodeSize(target_size);
      REQUIRE(v->size() == 46);



      THEN("We can extract the encoded size attribute again."){
        auto extracted_size = utility::uuidDecodeSize(v);
        REQUIRE((target_size == extracted_size));
      }

      WHEN("We manipulate the version size"){
        auto v2 = std::make_shared<const std::string>(*v + "123");
        THEN("Trying to extract the size attribute fails. "){
          REQUIRE_THROWS(utility::uuidDecodeSize(v2));
        }
      }
    }
  }
  
  GIVEN("a key"){
    auto key = std::make_shared<const string>("a-key");
    THEN("We can construct an indicator key"){
      auto indicator = utility::makeIndicatorKey(*key);
      
      AND_THEN("We can reconstruct the key from the indicator"){
        auto reconstructed = utility::indicatorToKey(*indicator);
        REQUIRE((*reconstructed == *key));
      }
    }
  }
  
  GIVEN("a fully qualified path"){
    auto url = "kinetic://cluster//the/path";
    auto path = utility::urlToPath(url);
    auto clusterId = utility::urlToClusterId(url);
    
    THEN("We can construct different types of keys"){
      auto mdkey = utility::makeMetadataKey(clusterId, path);
      REQUIRE((*mdkey == "cluster:metadata:/the/path"));

      auto attrkey = utility::makeAttributeKey(clusterId, path, "test-attribute");
      REQUIRE((*attrkey == "cluster:attribute:/the/path:test-attribute"));

      auto datakey = utility::makeDataKey(clusterId, path, 12);
      REQUIRE((*datakey == "cluster:data:/the/path_0000000012"));

      auto indicatorkey = utility::makeIndicatorKey(*datakey);
      REQUIRE((*indicatorkey == "indicator:"+ *datakey));

      AND_THEN("we can reconstruct the fully qualified url from the metadata key"){
        REQUIRE((utility::metadataToUrl(*mdkey) == url));
      }
      AND_THEN("we can extract the attribute name from the attribute key"){
        REQUIRE((utility::extractAttributeName(clusterId, path, *attrkey) == "test-attribute"));
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
      for(size_t i=0; i<stripe.size(); i++){
        crcs[i] = crc32(0, (const Bytef*)stripe[i]->c_str(), stripe[i]->length());
      }
      kio_notice("creating ", crcs.size(), " crc32 checksums took ",
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()-t).count(),
                " milliseconds ");

      t = std::chrono::system_clock::now();
      for(size_t i=0; i<stripe.size(); i++){
        crcs[i] = crc32c(0, stripe[i]->c_str(), stripe[i]->length());
      }

      kio_notice("creating ", crcs.size(), " crc32c checksums took ",
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()-t).count(),
                " milliseconds ");
    }
  }
};
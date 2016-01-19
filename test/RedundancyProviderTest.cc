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
#include "RedundancyProvider.hh"
#include "Utility.hh"
#include <fcntl.h>
#include <unistd.h>

using std::shared_ptr;
using std::string;
using std::make_shared;

using namespace kio;

const std::string value(
  "ISTANBUL — Confronted with widespread protests two summers ago, Prime Minister Recep Tayyip Erdogan ordered a harsh police crackdown and tarnished the demonstrators as traitors and spies. Faced with a corruption inquiry focused on his inner circle, he responded by purging the police and judiciary."
  "So when Mr. Erdogan, now president, suffered a stinging electoral defeat in June that left his party without a majority in Parliament and seemingly dashed his hopes of establishing an executive presidency, Turks were left wondering how he would respond."
  "Now many say they have their answer: a new war."
  "In resuming military operations against the separatist Kurdistan Workers' Party, or P.K.K., analysts see a calculated strategy for Mr. Erdogan's Islamist-rooted Justice and Development Party to regain its parliamentary majority in new elections."
  "Having already delayed the formation of a coalition government, analysts say, Mr. Erdogan is now buttressing his party's chances at winning new elections by appealing to Turkish nationalists opposed to self-determination for the Kurdish minority. Parallel to the military operations against the Kurds has been an effort to undermine the political side of the Kurdish movement, by associating it with the violence of the P.K.K., which has also seemed eager to return to fighting. The state battled the group for three decades at a cost of about 40,000 lives before a fragile peace process began in 2013."
  "The overall assumption is that President Erdogan wants to create the conditions so the result of June 7 can be overturned, so that he can run the country from the presidency, said Suat Kiniklioglu, a former lawmaker from Mr. Erdogan's party and the executive director of the Center for Strategic Communication, a research organization in Ankara."
  "Now a sharp critic of Mr. Erdogan, Mr. Kiniklioglu said, I think there is little debate among normal and sane people in Turkey that the war with the Kurds is being used as a tool to reverse the election defeat. The Justice and Development Party, known as the A.K.P., recently began conducting nationwide polls to see how it might fare in snap elections, which could be held as soon as November."
  "The outcome of these polls will be indicative of which direction they will go, Mr. Kiniklioglu said."
  "Many analysts say that after weeks of stalled coalition talks between the A.K.P. and three opposition parties, new elections are likely. And at a time of crisis, Turkish voters, experts say, could very well turn again to Mr. Erdogan and the A.K.P."
  "The results of a voter survey released Wednesday by a widely cited Turkish pollster found that Mr. Erdogan's party could regain a parliamentary majority if elections were held today."
);

std::vector<std::shared_ptr<const std::string> > makeStripe(int nData, int nParity, std::string value)
{
  size_t chunk_size = (value.size() + nData-1) / (nData);
  std::vector< std::shared_ptr<const std::string> > stripe;
  for(int i=0; i<nData+nParity; i++){
    if(i<nData){
      auto subchunk = std::make_shared<std::string>(value.substr(i*chunk_size, chunk_size));
      subchunk->resize(chunk_size); // ensure that all chunks are the same size
      stripe.push_back(std::move(subchunk));
    }
    else
      stripe.push_back(std::make_shared<std::string>());
  }
  return stripe;
}

SCENARIO("Redundancy Provider Test.", "[Redundancy]"){
    
  GIVEN ("A Replication Code") {
    int nData = 1;
    int nParity = 3; 
    RedundancyProvider rp(nData, nParity);
    auto stripe = makeStripe(nData, nParity, value);
    
    WHEN("We encoded Parity Information. "){
      REQUIRE_NOTHROW(rp.compute(stripe));

      THEN("Parities are replications."){
        for(int i=0; i<nData+nParity; i++)
          REQUIRE(value == *stripe[i]);  
      }
    }
  }

  GIVEN ("A 16-4 stripe configuration"){
    int nData = 16;
    int nParity = 4;
    RedundancyProvider rp(nData, nParity);

    THEN("We can run performance numbers."){

      for(int size = nData*1024*64; size <= nData*1024*1024; size*=2){

        int fd = open("/dev/urandom", O_RDONLY);
        std::vector<char> memory(size);
        read(fd, memory.data(), size);

        auto stripe = makeStripe(nData, nParity, std::string(memory.data(), size));

        int runs = 100;

        auto t = std::chrono::system_clock::now();
        for(auto i=0; i<runs; i++){
          rp.compute(stripe);
          for(auto j=nData; j<nData+nParity; j++)
            stripe[j] = std::make_shared<std::string>();
        }
        auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()-t).count();

        printf( "%d KB chunk size -> %ld MB/sec \n", size / (1024*nData), (long int) ((((size/1048576.0) * runs) / (milliseconds)) * 1000));
      }
    }
  }
    
  for(int nData=1; nData<=32; nData*=4){
    for(int nParity=0; nParity<=8; nParity+=2){
      
      GIVEN("Redundancy configuration: " + utility::Convert::toString(nData, "-", nParity)){
        RedundancyProvider rp(nData, nParity);
        auto stripe = makeStripe(nData, nParity, value);

        WHEN("We encoded Parity Information. "){

          REQUIRE_NOTHROW(rp.compute(stripe));

          THEN("We can reconstruct randomly deleted subchunks."){
            srand (time(NULL));
            for(int i=0; i<nParity; i++){
              auto indx = rand()%(nData+nParity);
              stripe[indx] = make_shared<const string>();
            }
            REQUIRE_NOTHROW(rp.compute(stripe));

            std::string reconstructed;
            for(int i=0; i<nData; i++){
              reconstructed.append(stripe[i]->c_str());
            }
            reconstructed.resize(value.size());
            REQUIRE(value == reconstructed);
          }
        }

        THEN("Too few healthy chunks throws."){
          stripe[0] = make_shared<const string>();
          REQUIRE_THROWS_AS(rp.compute(stripe), std::invalid_argument);
        }

        if(nData>1) THEN("Invalid chunk size throws."){
          std::string s = *stripe[0];
          s.append("This Chunk is too long.");
          stripe[0] = make_shared<const string>(s);
          REQUIRE_THROWS_AS(rp.compute(stripe), std::invalid_argument);
        }

        THEN("Invalid stripe size throws."){
          stripe.pop_back();
          REQUIRE_THROWS_AS(rp.compute(stripe), std::invalid_argument);
        }
      }
    }
  }
};
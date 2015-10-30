#include "catch.hpp"
#include "PrefetchOracle.hh"

using namespace kio; 


SCENARIO("PrefetchOracle Test", "[Prefetch]"){

  GIVEN ("An empty SPR object. "){
    PrefetchOracle spr(10);

    WHEN("if it has less than 3 elements"){
      THEN("it can't make any predictions"){
        REQUIRE(spr.predict(10).empty());
        spr.add(0);
        REQUIRE(spr.predict(10).empty());
        spr.add(1);
        REQUIRE(spr.predict(10).empty());
      }
    }
    
    WHEN("three elements are added"){
      spr.add(0);
      spr.add(2);
      spr.add(4);
      THEN("it can make a prediction"){
        REQUIRE(spr.predict(10).front() == 6);
      }
    }

    WHEN("a sequential rising sequence is added"){
      for(int i=0; i<20; i++)
        spr.add(i);

      THEN("Prediction size is limited if requested."){
        auto p = spr.predict(3);
        REQUIRE(p.size() == 3);
      }
      
      THEN("prediction size beyond max will not result in more predictions than the maximum"){
        auto p = spr.predict(15);
        REQUIRE(p.size() == 10);
      }
                
      THEN("prediction returns max elements"){
        auto p = spr.predict(10);
        REQUIRE(p.size() == 10);

        auto expected = 20;
        for(auto it = p.begin(); it != p.end(); it++)
          REQUIRE(*it == expected++);
        
        AND_THEN("Prediction result is not affected by adding the same number repeatedly"){
          for(int i=0; i<10; i++)
            spr.add(19);
          auto p2 = spr.predict(10);
          REQUIRE(p2.size() == p.size());
          REQUIRE(p2 == p);
        }
        
        AND_THEN("predicting again with CONTINUE set returns 0 elements"){
          REQUIRE(spr.predict(10,PrefetchOracle::PredictionType::CONTINUE).empty());
        }
        AND_THEN("Adding more elements to the sequence will result in prediction with CONTINUE set"){
          spr.add(20);
          auto p2 = spr.predict(10,PrefetchOracle::PredictionType::CONTINUE);
          REQUIRE(p2.size() == 1);
          REQUIRE(p2.front() == p.back()+1);
        }
      }
    }

    WHEN("We throw in outliers"){
      
      spr.add(59);
      for(int i=5; i<10; i++)
        spr.add(i);
      spr.add(99);

      THEN("we can still predict"){
        auto p = spr.predict(10);
        auto expected = 10;
        for(auto it = p.begin(); it != p.end(); it++)
          REQUIRE(*it == expected++);
      }
    }

    WHEN("sequence is skipping regularly"){
      for(int i=400; i>100; i-=10)
        spr.add(i);

      THEN("prediction takes gaps into account"){
        auto p = spr.predict(10);
        REQUIRE(p.size() == 10);

        auto expected = 100;
        for(auto it = p.begin(); it != p.end(); it++){
          REQUIRE(*it == expected);
          expected -= 10;
        }
      }
    }
  }
}
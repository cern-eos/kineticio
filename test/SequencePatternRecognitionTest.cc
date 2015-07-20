#include "catch.hpp"
#include "SequencePatternRecognition.hh"

using namespace kio; 


SCENARIO("SequencePatternRecognition Test", "[Pattern]"){

  GIVEN ("An empty SPR object. "){
    SequencePatternRecognition spr(10);

    WHEN("we let it stay empty"){
      THEN("it can't make any predictions"){
        auto p = spr.predict();
        REQUIRE(p.empty());
      }
    }

    WHEN("a sequential rising sequence is added"){
      for(int i=0; i<10; i++)
        spr.add(i);

      THEN("prediction returns max-1 elements"){
        auto p = spr.predict();
        REQUIRE(p.size() == 9);

        auto expected = 10;
        for(auto it = p.begin(); it != p.end(); it++)
          REQUIRE(*it == expected++);

        AND_THEN("Prediction result is not affected by adding the same number repeatedly"){
          for(int i=0; i<10; i++)
            spr.add(9);
          auto p2 = spr.predict();
          REQUIRE(p2.size() == p.size());
          REQUIRE(p2 == p);
        }
        
        AND_THEN("predicting again with CONTINUE set returns 0 elements"){
          REQUIRE(spr.predict(SequencePatternRecognition::PredictionType::CONTINUE).empty());
        }
        AND_THEN("Adding more elements to the sequence will result in prediction with CONTINUE set"){
          spr.add(10);
          auto p2 = spr.predict(SequencePatternRecognition::PredictionType::CONTINUE);
          REQUIRE(p2.size() == 1);
          REQUIRE(p2.front() == p.back()+1);
        }
      }
    }

    WHEN("only part of the sequence is sequential rising"){

      for(int i=100; i>90; i--)
        spr.add(i);
      for(int i=5; i<10; i++)
        spr.add(i);

      THEN("prediction is sequence_length-1 elements"){
        auto p = spr.predict();
        REQUIRE(p.size() == 4);

        auto expected = 10;
        for(auto it = p.begin(); it != p.end(); it++)
          REQUIRE(*it == expected++);
      }
    }

    WHEN("sequence is skipping regularly"){
      for(int i=200; i>100; i-=10)
        spr.add(i);

      THEN("prediction takes gaps into account"){
        auto p = spr.predict();
        REQUIRE(p.size() == 9);

        auto expected = 100;
        for(auto it = p.begin(); it != p.end(); it++){
          REQUIRE(*it == expected);
          expected -= 10;
        }
      }
    }
  }
}
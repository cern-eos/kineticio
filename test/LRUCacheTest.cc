#include "catch.hpp"
#include "LRUCache.hh"

using namespace kio;

SCENARIO("LRUCache Test.", "[LRU]")
{
  GIVEN ("An empty LRU cache") {
    LRUCache<int, std::string> cache(3);
    THEN("requesting an element throws"){
      REQUIRE_THROWS_AS(cache.get(4), std::out_of_range);
    }
  }
  GIVEN ("A filled LRU cache") {
    LRUCache<int, std::string> cache(3);
    cache.add(1, "one");
    cache.add(2, "two");
    cache.add(3, "three");

    THEN("An tail element can be retrieved."){
      REQUIRE(cache.get(1) == "one");
    }
    THEN("The head element can be retrieved."){
      REQUIRE(cache.get(3) == "three");
    }
    THEN("Requesting a nonexisting element throws"){
      REQUIRE_THROWS_AS(cache.get(4), std::out_of_range);
    }
    THEN("Adding an additional element throws out the tail element"){
      cache.add(4, "four");
      REQUIRE_THROWS_AS(cache.get(1), std::out_of_range);
    }
    THEN("Get changes LRU order."){
      cache.get(1);
      cache.add(4, "four");
      REQUIRE(cache.get(1) == "one");
      REQUIRE_THROWS_AS(cache.get(2), std::out_of_range);
    }
    WHEN("Adding the same element multiple times"){
      cache.add(3, "three");
      cache.add(3, "three");
      THEN("The abandoned zombies elements will have pushed out other entries."){
        REQUIRE_THROWS_AS(cache.get(1), std::out_of_range);
        REQUIRE_THROWS_AS(cache.get(2), std::out_of_range);
      }
      THEN("The element can be read as normal") {
        REQUIRE(cache.get(3) == "three");
      }
      THEN("Pushing out zombie elements works fine, the multiply added element remains accessible."){
        cache.add(4, "four");
        cache.add(5, "five");
        REQUIRE(cache.get(3) == "three");
        REQUIRE(cache.get(4) == "four");
        REQUIRE(cache.get(5) == "five");
      }
    }
  }
}
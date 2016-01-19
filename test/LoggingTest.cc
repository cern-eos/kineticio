#include "catch.hpp"
#include "Logging.hh"

using namespace kio;

SCENARIO("LoggingTest", "[log]"){

  GIVEN (""){
    THEN("We can log arbitrary length arbitrary type chains"){
      int i = 1;
      double d = 0.99;
      std::string s = "'happy'";
      kinetic::KineticStatus status (kinetic::StatusCode::OK, "Test message.");
      kio_notice("Logging Test: Integer ", i,", Double ", d, ", String ", s, ", KineticStatus ", status);
    }
  }
};
#include "catch.hpp"
#include "Logging.hh"

using namespace kio;

bool tshouldLog(const char *name, int level){
  return true;
}

bool tneverShouldLog(const char *name, int level){
  return false; 
}

static void tlog(const char* func, const char* file, int line, int priority, const char *msg)
{
  switch(priority) {
    case LOG_DEBUG:
      printf("DEBUG: ");
      break;
    case LOG_NOTICE:
      printf("NOTICE: ");
      break;
    case LOG_WARNING:
      printf("WARNING: ");
      break;
  }
  printf("%s /// %s (%s:%d)\n",msg,func,file,line);
}


SCENARIO("LoggingTest", "[log]"){

  GIVEN ("A registered log function"){
    THEN("We can log arbitrary length arbitrary type chains"){
      int i = 1;
      double d = 0.99;
      std::string s = "'happy'";
      kinetic::KineticStatus status (kinetic::StatusCode::OK, "Test message.");
      
      kio::Factory::registerLogFunction(tlog, tshouldLog);
      kio_notice("Logging Test: Integer ", i,", Double ", d, ", String ", s, ", KineticStatus ", status);
      kio::Factory::registerLogFunction(tlog, tneverShouldLog);
    }
  }
};
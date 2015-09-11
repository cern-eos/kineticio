#include "catch.hpp"
#include "Logging.hh"
#include <functional>

using namespace kio;

bool tshouldLog(const char *name, int level){
  return true;
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

class LogFunctionInitializer{
public:
    LogFunctionInitializer(){
      kio::Factory::registerLogFunction(tlog, tshouldLog);
    }
};
static LogFunctionInitializer kio_loginit;


SCENARIO("LoggingTest", "[log]"){

  GIVEN ("A registered log function"){
    THEN("We can log arbitrary length arbitrary type chains"){
      int i = 1;
      double d = 0.99;
      std::string s = "'happy'";
      kinetic::KineticStatus status (kinetic::StatusCode::OK, "Test message.");
      kio_notice("Integer ", i,", Double ", d, ", String ", s, ", KineticStatus ", status);
    }
  }
};
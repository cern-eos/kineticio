#include "catch.hpp"
#include "Logging.hh"
#include <functional>

using namespace kio;

bool tshouldLog(const char *name, int level){
  return true;
}

void tlog(const char* func, const char* file, int line, int level, const char* msg){
  printf("Level %d (%s:%s:%d) %s\n",level,file,func,line,msg);
}


SCENARIO("LoggingTest", "[log]"){

  GIVEN ("non registered log function") {
    THEN("Not logging a thing.") {
      kio_notice("This should not be printed.");
    }
  }

  GIVEN ("A factory registered log function"){

    kio::logfunc_t lf = tlog;
    kio::shouldlogfunc_t slf = tshouldLog;
    kio::Factory::registerLogFunction(lf, slf);

    THEN("We can log arbitrary length arbitrary type chains"){

      int i = 1;
      double d = 0.99;
      std::string s = "'happy'";
      kinetic::KineticStatus status (kinetic::StatusCode::OK, "Test message.");
      kio_notice("Integer: ", i," Double: ", d, " String: ", s, " KineticStatus: ", status);
    }
  }
};
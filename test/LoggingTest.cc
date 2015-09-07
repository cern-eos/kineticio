#include "catch.hpp"
#include "Logging.hh"
#include <functional>

using namespace kio;

bool tshouldLog(const char *name, int level){
  printf("Yes we should log %s at level %d\n",name,level);
  return true;
}

void tlog(const char* func, const char* file, int line, int level, const char* msg){
  printf("Level %d (%s:%s:%d) %s\n",level,file,func,line,msg);
}


SCENARIO("LoggingTest", "[log]"){

  GIVEN ("non registered log function") {
    THEN("Not logging a thing.") {
      printf("This won't log a thing:\n");
      kio_notice("a notice log");
    }
  }

  GIVEN ("A factory registered log function"){

    kio::logfunc_t lf = tlog;
    kio::shouldlogfunc_t slf = tshouldLog;
    kio::Factory::registerLogFunction(lf, slf);
    THEN("We can log"){
      printf("This will log: ");
      kio_notice("a notice log");
    }

    THEN("We can log arbitrary length arbitrary type chains"){
      kio_notice("Let's go ",1," and a doubel", 1.1, "and a string ", std::string("123"));
    }
  }
};
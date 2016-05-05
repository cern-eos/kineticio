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

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include <thread>
#include <sys/syslog.h>
#include "SimulatorController.h"
#include "KineticIoFactory.hh"

#define KNRM  "\x1B[0m"
#define KRED  "\x1B[31m"
#define KGRN  "\x1B[32m"
#define KYEL  "\x1B[33m"
#define KBLU  "\x1B[34m"
#define KMAG  "\x1B[35m"
#define KCYN  "\x1B[36m"
#define KWHT  "\x1B[37m"
#define RESET "\033[0m"

bool tshouldLog(const char *name, int level){
  return true;
}

static void tlog(const char* func, const char* file, int line, int priority, const char *msg)
{
  switch(priority) {
    case LOG_DEBUG:
      printf(KGRN "DEBUG ");
      break;
    case LOG_NOTICE:
      printf(KBLU "NOTICE ");
      break;
    case LOG_WARNING:
      printf(KYEL " WARNING ");
      break;
    case LOG_ERR:
      printf(KRED "ERROR ");
      break;
    default:
      printf(KMAG "Unknown Log Level! ");
  }

  std::stringstream ss;
  ss << std::this_thread::get_id();

  printf("%s@%s:%d",func,strrchr(file,'/')+1,line);

  printf(" (tid:%s) " RESET, ss.str().c_str() + ss.str().size() - 4);

  printf("%s  ",msg);
  printf("\n");
}

int main( int argc, char const* argv[] )
{
  /* Set environment variables so that KineticIo can find the test json configuration.
   * TESTJSON_LOCATION is set by cmake. */
  std::string location(getenv("KINETIC_DRIVE_LOCATION") ? getenv("KINETIC_DRIVE_LOCATION") : "" );
  setenv("KINETIC_DRIVE_LOCATION", TESTJSON_LOCATION, 1);
  std::string security(getenv("KINETIC_DRIVE_SECURITY") ? getenv("KINETIC_DRIVE_SECURITY") : "" );
  setenv("KINETIC_DRIVE_SECURITY", TESTJSON_LOCATION, 1);
  std::string cluster(getenv("KINETIC_CLUSTER_DEFINITION") ? getenv("KINETIC_CLUSTER_DEFINITION") : "" );
  setenv("KINETIC_CLUSTER_DEFINITION", TESTJSON_LOCATION, 1);

  /* Ignore sigpipe, so we don't die if a simulator is shut down. */
  signal(SIGPIPE, SIG_IGN);

  /* set logging to stdout if requested */
  if(strcmp(argv[argc-1], "-debug") == 0){
    kio::KineticIoFactory::registerLogFunction(tlog, tshouldLog);
    argc--;
  }

  int result = Catch::Session().run( argc, argv );

  /* Reset environment variables back to the initial values. */
  setenv("KINETIC_DRIVE_LOCATION", location.c_str(), 1);
  setenv("KINETIC_DRIVE_SECURITY", security.c_str(), 1);
  setenv("KINETIC_CLUSTER_DEFINITION", security.c_str(), 1);

  return result;
}
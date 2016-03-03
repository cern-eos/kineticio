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

/* Tests assume that location and security json file(s) exists and they contain information for
 * serial numbers SN1 and SN2, where SN1 is correct and SN2 is incorrect.
 * Preset is correct when project is built in a build folder in-source and a simulator is started on
 * localhost. */
#define KINETIC_DRIVE_LOCATION "../test/localhost.json"
#define KINETIC_DRIVE_SECURITY KINETIC_DRIVE_LOCATION
#define KINETIC_CLUSTER_DEFINITION KINETIC_DRIVE_LOCATION


bool tshouldLog(const char *name, int level){
  return true;
}

static void tlog(const char* func, const char* file, int line, int priority, const char *msg)
{
  switch(priority) {
    case LOG_DEBUG:
      printf("    DEBUG: ");
      break;
    case LOG_NOTICE:
      printf("  NOTICE: ");
      break;
    case LOG_WARNING:
      printf(" WARNING: ");
      break;
    case LOG_ERR:
      printf("ERROR: ");
      break;
    default:
      printf("Unknown Log Level! ");
  }
  printf("%s /// %s (%s:%d)\n",msg,func,file,line);
}

int main( int argc, char* const argv[] )
{
  // Set environment variables so that KineticIo can find the simulator.
  std::string location(getenv("KINETIC_DRIVE_LOCATION") ? getenv("KINETIC_DRIVE_LOCATION") : "" );
  setenv("KINETIC_DRIVE_LOCATION", KINETIC_DRIVE_LOCATION, 1);

  std::string security(getenv("KINETIC_DRIVE_SECURITY") ? getenv("KINETIC_DRIVE_SECURITY") : "" );
  setenv("KINETIC_DRIVE_SECURITY", KINETIC_DRIVE_SECURITY, 1);

  std::string cluster(getenv("KINETIC_CLUSTER_DEFINITION") ? getenv("KINETIC_CLUSTER_DEFINITION") : "" );
  setenv("KINETIC_CLUSTER_DEFINITION", KINETIC_CLUSTER_DEFINITION, 1);

  /* Ignore sigpipe, so we don't die if a simulator is shut down. */
  signal(SIGPIPE, SIG_IGN);


  /* set logging to stdout if requested */
  if(strcmp(argv[argc-1], "-debug") == 0){
    kio::KineticIoFactory::registerLogFunction(tlog, tshouldLog);
    argc--;
  }

  int result = Catch::Session().run( argc, (const char**) argv );

  // Reset environment variables back to the initial values.
  setenv("KINETIC_DRIVE_LOCATION", location.c_str(), 1);
  setenv("KINETIC_DRIVE_SECURITY", security.c_str(), 1);
  setenv("KINETIC_CLUSTER_DEFINITION", security.c_str(), 1);

  return result;
}
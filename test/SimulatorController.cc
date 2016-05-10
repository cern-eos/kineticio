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

#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include "Logging.hh"
#include "SimulatorController.h"



void SimulatorController::startSimulators(size_t capacity)
{
  if(pid) {
    throw std::runtime_error("Simulators already started.");
  }

  pid = fork();

  if (pid < 0) {
    pid=0;
    throw std::runtime_error("fork() returned error code.");
  }

  /* forked process */
  else if (pid == 0) {
    /* pipe simulator output to /dev/null */
    int fd = open("/dev/null", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    dup2(fd, 1);   // make stdout go to file
    dup2(fd, 2);   // make stderr go to file
    close(fd);     // fd no longer needed - the dup'ed handles are sufficient

    std::string classpath = kio::utility::Convert::toString(".:",TESTSIMULATOR_LOCATION,"/*");
    std::string capacity_str = kio::utility::Convert::toString(capacity);

    char* const args[] = {
        (char*) "/usr/bin/java",
        (char*) "-cp", (char*) classpath.c_str(),
        (char*) "com.seagate.kinetic.example.openstorage.VirtualDrives",
        (char*) capacity_str.c_str(),
        (char*) 0
    };
    execv("/usr/bin/java", args);
    throw std::runtime_error("Error in execv");
  }

  /* original process */
  else {
    kio_debug("Starting Simulator in process with pid ", pid, " using simulator directory ", TESTSIMULATOR_LOCATION);

    /* Wait for simulators to come up before continuing... */
    for(int i=0; i<10; i++){

      if(reset(capacity-1)){
        /* This extra wait really should be unnecessary, but let's just be extra certain that
         * the java process is up and running completely even on slow build nodes in order to
         * avoid false errors */
        usleep(5000*1000);
        return;
      }

      /* retry in a second */
      usleep(1000*1000);
    }
    throw std::runtime_error("Failed starting simulators.");
  }
}

void SimulatorController::stopSimulators()
{
  if(pid) {
    kio_debug("Killing Simulators");
    kill(pid, SIGTERM);
    pid = 0;
  }
}

bool SimulatorController::reset(size_t index)
{
  kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
  std::shared_ptr<kinetic::BlockingKineticConnection> con;
  if (factory.NewBlockingConnection(get(index), con, 30).ok()) {
    con->UnlockDevice("NULL");
    return con->InstantErase("NULL").ok();
  }
  return false;
}

bool SimulatorController::enable(size_t index)
{
  kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
  std::shared_ptr<kinetic::BlockingKineticConnection> con;
  if (factory.NewBlockingConnection(get(index), con, 30).ok()) {
    return con->UnlockDevice("NULL").ok();
  }
  return false;
}

bool SimulatorController::block(size_t index)
{
  kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
  std::shared_ptr<kinetic::BlockingKineticConnection> con;
  if (factory.NewBlockingConnection(get(index), con, 30).ok()) {
    return con->LockDevice("NULL").ok();
  }
  return false;
}

kinetic::ConnectionOptions SimulatorController::get(int index)
{
  return kinetic::ConnectionOptions{"localhost", 18123 + index, true, 1, "asdfasdf"};
}

SimulatorController::SimulatorController()
{ }

SimulatorController::~SimulatorController()
{
  if(pid) {
    stopSimulators();
  }
}

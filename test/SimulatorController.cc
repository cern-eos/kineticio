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
#include "Logging.hh"
#include "SimulatorController.h"

bool SimulatorController::start(int index)
{
  if(pids.size()>index)
    if(pids[index])
      return false;

  int pid;
  if ((pid = fork()) < 0) {
    return false;
  }
  else if (pid == 0) {
    std::string home = "tmp"+std::to_string((long long int) index);
    std::string port = std::to_string((long long int) 8123 + index);
    std::string tls  = std::to_string((long long int) 8443 + index);
    char* const args[] = {
          (char*)"startSimulator.sh",
          (char*)"-port", (char*)port.c_str(),
          (char*)"-tlsport", (char*)tls.c_str(),
          (char*)"-home", (char*)home.c_str(),
          (char*)0
    };
    execv("vendor/src/kinetic_simulator/bin/startSimulator.sh", args);
  }
  else {
    if(pids.size()<index+1)
      pids.resize(index+1);
    pids[index] = pid;
    kio_debug("Starting Simulator on port ", 8123+index, "with pid ",pids[index]);
    usleep(1000 * 2000);
    return true;
  }
}

bool SimulatorController::stop(int index)
{
  if(pids.size()<=index || !pids[index])
    return false;

  kio_debug("Killing Simulator on port ", 8123+index, "with pid ",pids[index]);
  kill(pids[index], SIGTERM);

  pids[index]=0;
  usleep(1000 * 1000);
  return true;
}

bool SimulatorController::reset(int index)
{
  kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
  std::shared_ptr<kinetic::BlockingKineticConnection> con;
  if( factory.NewBlockingConnection(get(index), con, 30).ok() )
    return con->InstantErase("NULL").ok();
  return false;
}

kinetic::ConnectionOptions SimulatorController::get(int index) {
  return kinetic::ConnectionOptions{ "localhost", 8443+index, true, 1, "asdfasdf" };
}

SimulatorController::SimulatorController()
{}

SimulatorController::~SimulatorController()
{
  for(int i=0; i<pids.size(); i++)
    stop(i);
}

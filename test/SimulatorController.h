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

#ifndef KINETICIO_SIMULATORCONTROLLER_H
#define KINETICIO_SIMULATORCONTROLLER_H

#include <kinetic/kinetic.h>
#include <vector>

class SimulatorController {
public:
  void startSimulators(size_t capacity);
  void stopSimulators();

  bool enable(size_t index);
  bool block(size_t index);
  bool reset(size_t index);

  kinetic::ConnectionOptions get(int index);

  static SimulatorController& getInstance(){
    static SimulatorController sc;
    return sc;
  }

  ~SimulatorController();

private:
  int capacity;
  int pid;
  SimulatorController();
};

#endif //KINETICIO_SIMULATORCONTROLLER_H

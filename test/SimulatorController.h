//
// Created by plensing on 7/27/15.
//

#ifndef KINETICIO_SIMULATORCONTROLLER_H
#define KINETICIO_SIMULATORCONTROLLER_H

#include <kinetic/kinetic.h>
#include <vector>

class SimulatorController {
public:
  bool start(int index);
  bool stop(int index);
  bool reset(int index);
  kinetic::ConnectionOptions get(int index);

  static SimulatorController& getInstance(){
    static SimulatorController sc;
    return sc;
  }

  ~SimulatorController();

private:
  SimulatorController();
  std::vector<int> pids;
};

#endif //KINETICIO_SIMULATORCONTROLLER_H

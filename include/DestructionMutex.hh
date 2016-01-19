//------------------------------------------------------------------------------
//! @file DestructionMutex.hh
//! @author Paul Hermann Lensing
//! @brief A mutex specialization that throws on lock if destructed is set.
//------------------------------------------------------------------------------

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

#ifndef KINETICIO_DESTRUCTIONMUTEX_HH
#define	KINETICIO_DESTRUCTIONMUTEX_HH

#include <mutex>

namespace kio {

//------------------------------------------------------------------------------
//! Is a BasicLockable, providing lock() and unlock() methods.
//------------------------------------------------------------------------------
class DestructionMutex {

public:
  void setDestructed()
  {
    std::lock_guard<std::mutex> lock(mutex);
    _destructed = true;
  }

  void lock()
  {
    mutex.lock();
    if (_destructed) {
      mutex.unlock();
      throw std::logic_error("Locking destructed object ist invalid.");
    }
  }

  void unlock()
  {
    mutex.unlock();
  }

  DestructionMutex() : _destructed(false) { };

private:
  bool _destructed;
  std::mutex mutex;
};

}

#endif

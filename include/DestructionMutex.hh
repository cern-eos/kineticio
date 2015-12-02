//------------------------------------------------------------------------------
//! @file DestructionMutex.hh
//! @author Paul Hermann Lensing
//! @brief A mutex specialization that throws on lock if destructed is set.
//------------------------------------------------------------------------------
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

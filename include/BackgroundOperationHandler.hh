//------------------------------------------------------------------------------
//! @file BackgroundOperationHandler.hh
//! @author Paul Hermann Lensing
//! @brief Execute background operations, spawning a limited number of concurrent threads.
//------------------------------------------------------------------------------
#ifndef KINETICIO_BACKGROUNDOPERATIONHANDLER_HH
#define KINETICIO_BACKGROUNDOPERATIONHANDLER_HH

/* <cstdatomic> is part of gcc 4.4.x experimental C++0x support... <atomic> is
 * what actually made it into the standard.*/
#if __GNUC__ == 4 && (__GNUC_MINOR__ == 4)
    #include <cstdatomic>
#else
  #include <atomic>
#endif
#include <functional>

namespace kio {

//------------------------------------------------------------------------------
//! Execute a supplied function asynchronously in a different thread while
//! controlling maximum concurrency.
//------------------------------------------------------------------------------
class BackgroundOperationHandler {
public:
  //--------------------------------------------------------------------------
  //! Execute supplied function. If thread-limit is not exceeded, execution
  //! is done asynchronously. However, if thread-limit is exceeded, the
  //! supplied function is executed in the _calling_ thread.
  //!
  //! @param function the function to be executed.
  //--------------------------------------------------------------------------
  void run(std::function<void()> function);

  //--------------------------------------------------------------------------
  //! Execute supplied function. If thread-limit is not exceeded, execution
  //! is done asynchronously. However, if thread-limit is exceeded, the
  //! supplied function is _not_ executed.
  //!
  //! @param function the function to be executed.
  //! @return true if function is being executed, false otherwise
  //--------------------------------------------------------------------------
  bool try_run(std::function<void()> function);

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param max_concurrent maximum number of concurrently spawned background
  //!   threads
  //--------------------------------------------------------------------------
  explicit BackgroundOperationHandler(int max_concurrent);

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~BackgroundOperationHandler();

private:
  //--------------------------------------------------------------------------
  //! Threadsafe wrapper executing supplied function and counting thread use
  //!
  //! @param function function to execute
  //--------------------------------------------------------------------------
  void execute(std::function<void()> function);

private:
  //! maximum number of background threads spawned concurrently
  const int thread_capacity;
  //! current number of active background threads
  std::atomic<int> numthreads;
};

}

#endif //KINETICIO_BACKGROUNDOPERATIONHANDLER_HH

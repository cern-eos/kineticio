#include "BackgroundOperationHandler.hh"
#include <thread>
#include <unistd.h>

using namespace kio;

BackgroundOperationHandler::BackgroundOperationHandler(int max_concurrent) :
    thread_capacity(max_concurrent), numthreads(0)
{ }

BackgroundOperationHandler::~BackgroundOperationHandler()
{
  // Ensure all background threads have terminated before destructing the object.
  // gcc 4.4.7 doesn't bring this_thread::sleep_for, so we just use usleep to wait.
  while (numthreads.load()) {
    usleep(1000);
  }
}

void BackgroundOperationHandler::execute(std::function<void()> function) noexcept
{
  try {
    function();
  }
  catch (...) { }
  numthreads--;
}

bool BackgroundOperationHandler::try_run(std::function<void()> function)
{
  if (numthreads.load() >= thread_capacity)
    return false;

  numthreads++;
  std::thread(&BackgroundOperationHandler::execute, this, std::move(function)).detach();
  return true;
}

void BackgroundOperationHandler::run(std::function<void()> function)
{
  if (!try_run(function))
    function();
}
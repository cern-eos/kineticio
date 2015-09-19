#include "BackgroundOperationHandler.hh"
#include <thread>
#include <unistd.h>
#include <Logging.hh>

using namespace kio;


BackgroundOperationHandler::BackgroundOperationHandler(int worker_threads, int queue_depth) :
   queue_capacity(queue_depth), thread_capacity(worker_threads), numthreads(0), shutdown(false)
{
  if(queue_capacity)
  for(int i=0; i<worker_threads; i++)
    std::thread(&BackgroundOperationHandler::worker_thread, this).detach();
}

BackgroundOperationHandler::~BackgroundOperationHandler()
{
  shutdown = true;
  worker.notify_all();

  // Ensure all background threads have terminated before destructing the object.
  while (numthreads)
    usleep(1000);
}

void BackgroundOperationHandler::worker_thread()
{
  numthreads++;
  std::function<void()> function;
  while(true){
    {
      std::unique_lock<std::mutex> lck(queue_mutex);
      while (q.empty() && !shutdown)
        worker.wait(lck);
      if(shutdown)
        break;
      function = q.front();
      q.pop();
    }
    controller.notify_one();
    try {
      function();
    }
    catch (const std::exception& e) {
      kio_warning("Exception in background worker thread: ", e.what());
    }
    catch (...) {
      kio_warning("Something that is not an exception threw!");
    }
  }
  numthreads--;
}

void BackgroundOperationHandler::run(std::function<void()>&& function)
{
  if(!queue_capacity)
    return run_noqueue(std::move(function));
  {
    std::lock_guard<std::mutex> lck(queue_mutex);
    q.push(std::move(function));
  }
  worker.notify_one();

  std::unique_lock<std::mutex> lck(queue_mutex);
  while (q.size() > queue_capacity)
    controller.wait(lck);
}

bool BackgroundOperationHandler::try_run(std::function<void()>&& function)
{
  if(!queue_capacity)
    return try_run_noqueue(std::move(function));
  {
    std::lock_guard<std::mutex> lck(queue_mutex);
    if(q.size() >= queue_capacity)
      return false;
    q.push(std::move(function));
  }
  worker.notify_one();
  return true;
}

void BackgroundOperationHandler::execute_noqueue(std::function<void()> function)
{
  try {
    function();
  }
  catch (...) { }
  numthreads--;
}

bool BackgroundOperationHandler::try_run_noqueue(std::function<void()> function)
{
  if (numthreads.load() >= thread_capacity)
    return false;

  numthreads++;
  std::thread(&BackgroundOperationHandler::execute_noqueue, this, std::move(function)).detach();
  return true;
}

void BackgroundOperationHandler::run_noqueue(std::function<void()> function)
{
  if (!try_run_noqueue(function))
    function();
}
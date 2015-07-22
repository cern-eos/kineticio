#include <KineticCallbacks.hh>

using namespace kio;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

CallbackSynchronization::CallbackSynchronization() : outstanding(0), trigger(0), cv(), mutex() { }

CallbackSynchronization::~CallbackSynchronization() { }

void CallbackSynchronization::wait_until(std::chrono::system_clock::time_point timeout_time, int min_outstanding)
{
  if (min_outstanding < 0) throw std::invalid_argument("negative outstanding");
  std::unique_lock<std::mutex> lck(mutex);
  trigger = min_outstanding;
  while (outstanding > trigger && std::chrono::system_clock::now() < timeout_time)
    cv.wait_until(lck, timeout_time);
}

void CallbackSynchronization::increment()
{
  std::lock_guard<std::mutex> lck(mutex);
  outstanding++;
}

void CallbackSynchronization::decrement()
{
  std::unique_lock<std::mutex> lck(mutex);
  outstanding--;
  if (outstanding <= trigger) {
    lck.unlock();
    cv.notify_one();
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

KineticCallback::KineticCallback(std::shared_ptr<CallbackSynchronization> s) :
    status(kinetic::KineticStatus(kinetic::StatusCode::CLIENT_INTERNAL_ERROR, "no result")),
    sync(std::move(s)),
    done(false)
{
  sync->increment();
}

KineticCallback::~KineticCallback() { }

void KineticCallback::OnResult(kinetic::KineticStatus result)
{
  status = result;
  done = true;
  sync->decrement();
}

kinetic::KineticStatus &KineticCallback::getResult()
{
  return status;
}

bool KineticCallback::finished()
{
  return done;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

using namespace kinetic;

GetCallback::GetCallback(std::shared_ptr<CallbackSynchronization> &s) : KineticCallback(s) { }

GetCallback::~GetCallback() { }

void GetCallback::Success(const std::string &key, std::unique_ptr<kinetic::KineticRecord> r)
{
  record = std::move(r);
  OnResult(KineticStatus(StatusCode::OK, ""));
}

void GetCallback::Failure(KineticStatus error)
{
  OnResult(error);
};

const std::unique_ptr<KineticRecord> &GetCallback::getRecord()
{
  return record;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

GetVersionCallback::GetVersionCallback(std::shared_ptr<CallbackSynchronization> &s) : KineticCallback(s) { }

GetVersionCallback::~GetVersionCallback() { }

void GetVersionCallback::Success(const std::string &v)
{
  version = v;
  OnResult(KineticStatus(StatusCode::OK, ""));
}

void GetVersionCallback::Failure(KineticStatus error)
{
  OnResult(error);
};

const std::string &GetVersionCallback::getVersion()
{
  return version;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

GetLogCallback::GetLogCallback(shared_ptr<CallbackSynchronization> &s) : KineticCallback(s) { }

GetLogCallback::~GetLogCallback() { }

void GetLogCallback::Success(unique_ptr<DriveLog> dlog)
{
  drive_log = std::move(dlog);
  OnResult(KineticStatus(StatusCode::OK, ""));
}

void GetLogCallback::Failure(KineticStatus error)
{
  OnResult(error);
}

unique_ptr<DriveLog> &GetLogCallback::getLog()
{
  return drive_log;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

PutCallback::PutCallback(shared_ptr<CallbackSynchronization> &s) : KineticCallback(s) { }

PutCallback::~PutCallback() { }

void PutCallback::Success()
{
  OnResult(KineticStatus(StatusCode::OK, ""));
}

void PutCallback::Failure(KineticStatus error)
{
  OnResult(error);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

DeleteCallback::DeleteCallback(shared_ptr<CallbackSynchronization> &s) : KineticCallback(s) { }

DeleteCallback::~DeleteCallback() { }

void DeleteCallback::Success()
{
  OnResult(KineticStatus(StatusCode::OK, ""));
}

void DeleteCallback::Failure(KineticStatus error)
{
  OnResult(error);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

RangeCallback::RangeCallback(shared_ptr<CallbackSynchronization> &s) : KineticCallback(s) { }

RangeCallback::~RangeCallback() { }

void RangeCallback::Success(unique_ptr<vector<string>> k)
{
  keys = std::move(k);
  OnResult(KineticStatus(StatusCode::OK, ""));
}

void RangeCallback::Failure(KineticStatus error)
{
  OnResult(error);
};

unique_ptr<vector<string> > &RangeCallback::getKeys()
{
  return keys;
}
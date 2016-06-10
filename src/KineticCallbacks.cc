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

#include <KineticCallbacks.hh>

using namespace kio;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

CallbackSynchronization::CallbackSynchronization() : outstanding(0), cv(), mutex()
{ }

CallbackSynchronization::~CallbackSynchronization()
{ }

void CallbackSynchronization::wait_until(std::chrono::system_clock::time_point timeout_time)
{
  std::unique_lock<std::mutex> lck(mutex);
  while (outstanding && std::chrono::system_clock::now() < timeout_time) {
    cv.wait_until(lck, timeout_time);
  }
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

KineticCallback::KineticCallback(std::shared_ptr<CallbackSynchronization> s) :
    status(kinetic::KineticStatus(kinetic::StatusCode::CLIENT_INTERNAL_ERROR, "no result")),
    sync(std::move(s)),
    done(false)
{
  sync->outstanding++;
}

KineticCallback::~KineticCallback()
{ }

void KineticCallback::OnResult(kinetic::KineticStatus result)
{
  std::unique_lock<std::mutex> lock(sync->mutex);
  if (done) {
    return;
  }

  status = result;
  done = true;
  sync->outstanding--;
  if (!sync->outstanding) {
    sync->cv.notify_one();
  }
}

kinetic::KineticStatus& KineticCallback::getResult()
{
  std::lock_guard<std::mutex> lock(sync->mutex);
  return status;
}

bool KineticCallback::finished()
{
  std::lock_guard<std::mutex> lock(sync->mutex);
  return done;
}

void KineticCallback::reset()
{
  std::lock_guard<std::mutex> lock(sync->mutex);
  done = false;
  status = kinetic::KineticStatus(kinetic::StatusCode::CLIENT_INTERNAL_ERROR, "no result");
  sync->outstanding++;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

using namespace kinetic;

GetCallback::GetCallback(std::shared_ptr<CallbackSynchronization> s) : KineticCallback(std::move(s))
{ }

GetCallback::~GetCallback()
{ }

void GetCallback::Success(const std::string& key, std::unique_ptr<kinetic::KineticRecord> r)
{
  record = std::move(r);
  OnResult(KineticStatus(StatusCode::OK, ""));
}

void GetCallback::Failure(KineticStatus error)
{
  OnResult(error);
};

const std::unique_ptr<KineticRecord>& GetCallback::getRecord()
{
  return record;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

GetVersionCallback::GetVersionCallback(std::shared_ptr<CallbackSynchronization> s) : KineticCallback(std::move(s))
{ }

GetVersionCallback::~GetVersionCallback()
{ }

void GetVersionCallback::Success(const std::string& v)
{
  version = v;
  OnResult(KineticStatus(StatusCode::OK, ""));
}

void GetVersionCallback::Failure(KineticStatus error)
{
  OnResult(error);
};

const std::string& GetVersionCallback::getVersion()
{
  return version;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

GetLogCallback::GetLogCallback(std::shared_ptr<CallbackSynchronization> s) : KineticCallback(std::move(s))
{ }

GetLogCallback::~GetLogCallback()
{ }

void GetLogCallback::Success(unique_ptr<DriveLog> dlog)
{
  drive_log = std::move(dlog);
  OnResult(KineticStatus(StatusCode::OK, ""));
}

void GetLogCallback::Failure(KineticStatus error)
{
  OnResult(error);
}

unique_ptr<DriveLog>& GetLogCallback::getLog()
{
  return drive_log;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

PutCallback::PutCallback(std::shared_ptr<CallbackSynchronization> s) : KineticCallback(std::move(s))
{ }

PutCallback::~PutCallback()
{ }

void PutCallback::Success()
{
  OnResult(KineticStatus(StatusCode::OK, ""));
}

void PutCallback::Failure(KineticStatus error)
{
  OnResult(error);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

BasicCallback::BasicCallback(std::shared_ptr<CallbackSynchronization> s) : KineticCallback(std::move(s))
{ }

BasicCallback::~BasicCallback()
{ }

void BasicCallback::Success()
{
  OnResult(KineticStatus(StatusCode::OK, ""));
}

void BasicCallback::Failure(KineticStatus error)
{
  OnResult(error);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

RangeCallback::RangeCallback(std::shared_ptr<CallbackSynchronization> s) : KineticCallback(std::move(s))
{ }

RangeCallback::~RangeCallback()
{ }

void RangeCallback::Success(unique_ptr<vector<string>> k)
{
  keys = std::move(k);
  OnResult(KineticStatus(StatusCode::OK, ""));
}

void RangeCallback::Failure(KineticStatus error)
{
  OnResult(error);
};

unique_ptr<vector<string> >& RangeCallback::getKeys()
{
  return keys;
}
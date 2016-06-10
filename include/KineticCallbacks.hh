//------------------------------------------------------------------------------
//! @file KineticCallbacks.hh
//! @author Paul Hermann Lensing
//! @brief Provide callback & synchronization functionality for asynchronous 
//!        kinetic operations.
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

#ifndef KINETICIO_OPERATIONCALLBACKS_HH
#define  KINETICIO_OPERATIONCALLBACKS_HH

#include <kinetic/kinetic.h>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>
#include <string>

namespace kio {

//------------------------------------------------------------------------------
//! Synchronization between multiple KineticCallback entities waiting on
//! completion. Primarily offer wait_until functionality.
//------------------------------------------------------------------------------
class CallbackSynchronization {
  friend class KineticCallback;

public:
  //----------------------------------------------------------------------------
  //! Blocking wait until either the timeout point has passed or the number of
  //! outstanding results reaches the supplied minimum.
  //!
  //! @param timeout_time the point of time the function is guaranteed to return
  //! @param min_outstanding the number of outstanding results to wait for
  //----------------------------------------------------------------------------
  void wait_until(std::chrono::system_clock::time_point timeout_time);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  explicit CallbackSynchronization();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~CallbackSynchronization();

private:
  //! the number of currently outstanding requests
  int outstanding;
  //! condition variable for wait_until functionality
  std::condition_variable cv;
  //! mutex for condition variable and thread safety
  std::mutex mutex;
};

//------------------------------------------------------------------------------
//! The base class of all callback used by KineticCluster, providing a unified
//! interface to check for completion and return status.
//------------------------------------------------------------------------------
class KineticCallback {
public:
  //----------------------------------------------------------------------------
  //! Marks the callback as finished and sets the supplied result. This function
  //! is called by all subclasses of KineticCallback, but also can be called
  //! directly in case of errors (e.g. connection timeout).
  //!
  //! @param result the result of the operation this callback belongs to.
  //----------------------------------------------------------------------------
  void OnResult(kinetic::KineticStatus result);

  //----------------------------------------------------------------------------
  //! Obtain the result of the operation this callback belongs to. Note that
  //! the returned result will only be valid if finished()==true
  //!
  //! @param result the result of the operation this callback belongs to.
  //----------------------------------------------------------------------------
  kinetic::KineticStatus& getResult();

  void reset();

  //----------------------------------------------------------------------------
  //! Check if the callback has been called (and the operation thus completed)
  //!
  //! @return true if the operation has completed, false otherwise.
  //----------------------------------------------------------------------------
  bool finished();

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  KineticCallback(std::shared_ptr<CallbackSynchronization> s);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~KineticCallback();

private:
  //! the status / result of the kinetic operation this callback belongs to
  kinetic::KineticStatus status;
  //! count outstanding operations and wake blocked thread when all are ready
  std::shared_ptr<CallbackSynchronization> sync;
  //! true if the associated kinetic operation has completed, false otherwise
  bool done;
};

class GetCallback : public KineticCallback, public kinetic::GetCallbackInterface {
public:
  const std::unique_ptr<kinetic::KineticRecord>& getRecord();

  void Success(const std::string& key, std::unique_ptr<kinetic::KineticRecord> r);

  void Failure(kinetic::KineticStatus error);

  explicit GetCallback(std::shared_ptr<CallbackSynchronization> s);

  ~GetCallback();

private:
  std::unique_ptr<kinetic::KineticRecord> record;
};

class GetVersionCallback : public KineticCallback, public kinetic::GetVersionCallbackInterface {
public:
  const std::string& getVersion();

  void Success(const std::string& v);

  void Failure(kinetic::KineticStatus error);

  explicit GetVersionCallback(std::shared_ptr<CallbackSynchronization> s);

  ~GetVersionCallback();

private:
  std::string version;
};

class GetLogCallback : public KineticCallback, public kinetic::GetLogCallbackInterface {
public:
  std::unique_ptr<kinetic::DriveLog>& getLog();

  void Success(std::unique_ptr<kinetic::DriveLog> dlog);

  void Failure(kinetic::KineticStatus error);

  explicit GetLogCallback(std::shared_ptr<CallbackSynchronization> s);

  ~GetLogCallback();

private:
  std::unique_ptr<kinetic::DriveLog> drive_log;
};

class PutCallback : public KineticCallback, public kinetic::PutCallbackInterface {
public:
  void Success();

  void Failure(kinetic::KineticStatus error);

  explicit PutCallback(std::shared_ptr<CallbackSynchronization> s);

  ~PutCallback();
};

class BasicCallback : public KineticCallback, public kinetic::SimpleCallbackInterface {
public:
  void Success();

  void Failure(kinetic::KineticStatus error);

  explicit BasicCallback(std::shared_ptr<CallbackSynchronization> s);

  ~BasicCallback();
};

class RangeCallback : public KineticCallback, public kinetic::GetKeyRangeCallbackInterface {
public:
  std::unique_ptr<std::vector<std::string> >& getKeys();

  void Success(std::unique_ptr<std::vector<std::string>> k);

  void Failure(kinetic::KineticStatus error);

  explicit RangeCallback(std::shared_ptr<CallbackSynchronization> s);

  ~RangeCallback();

private:
  std::unique_ptr<std::vector<std::string> > keys;
};

}

#endif	/* OPERATIONCALLBACKS_HH */


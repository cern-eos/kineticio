//------------------------------------------------------------------------------
//! @file KineticAutoConnection.hh
//! @author Paul Hermann Lensing
//! @brief Wrapping kinetic connection, primarily to supply automatic reconnect
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

#ifndef KINETICIO_AUTOCONNECTION_HH
#define	KINETICIO_AUTOCONNECTION_HH

#include <kinetic/kinetic.h>
#include <utility>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include "SocketListener.hh"
#include "BackgroundOperationHandler.hh"
#include "DestructionMutex.hh"

namespace kio{

//------------------------------------------------------------------------------
//! Wrapping kinetic::ThreadsafeNonblockingKineticConnection, (re)connecting
//! automatically when the underlying connection is requested.
//------------------------------------------------------------------------------
class KineticAutoConnection {
public:
  //--------------------------------------------------------------------------
  //! Set the connection error status if an operation on the connection
  //! failed catastrophically.
  //!
  //! @param errorConnection the underlying connection for which an error was
  //!   observed.
  //--------------------------------------------------------------------------
  void setError(std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection>& errorConnection);

  //--------------------------------------------------------------------------
  //! Return copy of underlying connection pointer, reconnect if indicated by
  //! current status and allowed by rate limit, throws if
  //! connection is not usable.
  //!
  //! @return copy of underlying connection pointer
  //--------------------------------------------------------------------------
  std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection> get();

  //--------------------------------------------------------------------------
  //! Return human readable name of the auto connection. 
  //--------------------------------------------------------------------------
  const std::string& getName() const;
  
  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param options host / port / key of target kinetic drive
  //! @param ratelimit minimum time between reconnection attempts
  //! @param min_getlog_interval minimum time between getlog attempts
  //--------------------------------------------------------------------------
  KineticAutoConnection(
      SocketListener& sockwatch,
      std::pair< kinetic::ConnectionOptions, kinetic::ConnectionOptions > options,
      std::chrono::seconds ratelimit
  );

  //--------------------------------------------------------------------------
  //! Destructor.
  //--------------------------------------------------------------------------
  ~KineticAutoConnection();

private:
  //! the two interfaces of the target drive, first interface will be prioritized
  const std::pair< kinetic::ConnectionOptions, kinetic::ConnectionOptions > options;
  //! minimum time between reconnection attempts
  const std::chrono::seconds ratelimit;
  //! the underlying connection
  std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection> connection;
  //! healthy if the underlying connection is believed to be currently working
  bool healthy;
  //! the fd of an open connection
  int fd;
  //! string representation of connection options for logging purposes
  std::string logstring;
  //! timestamp of the last connection attempt
  std::chrono::system_clock::time_point timestamp;
  //! thread safety
  std::mutex mutex;
  //! use calling thread for initial connect
  std::once_flag intial_connect;
  //! register connections with epoll listener
  SocketListener& sockwatch;
  //! random number generator
  std::mt19937 mt;
  //! background operation handler. last initialized, first destructed, guaranteeing that no
  //! background threads exist past any other member variable destruction
  BackgroundOperationHandler bg;

private:
  //--------------------------------------------------------------------------
  //! Attempt to connect unless blocked by rate limit. Will attempt both host
  //! names supplied to options and prioritize randomly.
  //--------------------------------------------------------------------------
  void connect();
};

}

#endif	


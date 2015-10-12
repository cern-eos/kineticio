//------------------------------------------------------------------------------
//! @file KineticAutoConnection.hh
//! @author Paul Hermann Lensing
//! @brief Wrapping kinetic connection, primarily to supply automatic reconnect
//------------------------------------------------------------------------------
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
  //! @param status the error status that occurred.
  //--------------------------------------------------------------------------
  void setError();

  //--------------------------------------------------------------------------
  //! Return copy of underlying connection pointer, reconnect if indicated by
  //! current status and allowed by rate limit, throws if
  //! connection is not usable.
  //!
  //! @return copy of underlying connection pointer
  //--------------------------------------------------------------------------
  std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection> get();

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
  //! the underlying connection
  std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection> connection;
  //! healthy if the underlying connection is believed to be currently working
  bool healthy;
  //! the fd of an open connection
  int fd;
  //! the two interfaces of the target drive, first interface will be prioritized
  std::pair< kinetic::ConnectionOptions, kinetic::ConnectionOptions > options;
  //! string representation of connection options for logging purposes
  std::string logstring;
  //! timestamp of the last connection attempt
  std::chrono::system_clock::time_point timestamp;
  //! minimum time between reconnection attempts
  std::chrono::seconds ratelimit;
  //! thread safety
  std::mutex mutex;
  //! handle reconnects in the background
  BackgroundOperationHandler bg;
  //! use calling thread for initial connect
  std::once_flag intial_connect;
  //! register connections with epoll listener
  SocketListener& sockwatch;
  //! random number generator
  std::mt19937 mt;

private:
  //--------------------------------------------------------------------------
  //! Attempt to connect unless blocked by rate limit. Will attempt both host
  //! names supplied to options and prioritize randomly.
  //--------------------------------------------------------------------------
  void connect();
};

}

#endif	


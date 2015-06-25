#ifndef RATELIMITKINETICCONNECTION_HH
#define	RATELIMITKINETICCONNECTION_HH

#include <kinetic/kinetic.h>
#include <utility>
#include <chrono>
#include <memory>
#include <mutex>

class RateLimitKineticConnection {
public:
  //--------------------------------------------------------------------------
  //! Set the connection error status if an operation on the connection
  //! failed catastrophically.
  //!
  //! @param status The error status that ocurred.
  //--------------------------------------------------------------------------
  void setError(kinetic::KineticStatus status);

  //--------------------------------------------------------------------------
  //! Return copy of underlying connection pointer, reconnect if indicated by
  //! current status and allowed by rate limit, throws KineticException if
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
  RateLimitKineticConnection(std::pair< kinetic::ConnectionOptions, kinetic::ConnectionOptions > options, std::chrono::seconds ratelimit);

  //--------------------------------------------------------------------------
  //! Destructor.
  //--------------------------------------------------------------------------
  ~RateLimitKineticConnection();

private:
  //! the underlying connection
  std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection> connection;
  //! the two interfaces of the target drive, first interface will be prioritized
  std::pair< kinetic::ConnectionOptions, kinetic::ConnectionOptions > options;
  //! timestamp of the last connection attempt
  std::chrono::system_clock::time_point timestamp;
  //! minimum time between reconnection attempts
  std::chrono::seconds ratelimit;
  //! status of last reconnect attempt
  kinetic::KineticStatus status;
  
  //! thread safety. TODO: this shouldn't be a shared_ptr, need to write copy constructor 
  std::shared_ptr<std::mutex> mutex;

private:
  void connect();
};



#endif	/* RATELIMITKINETICCONNECTION_HH */


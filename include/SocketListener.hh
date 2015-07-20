//------------------------------------------------------------------------------
//! @file SocketListener.hh
//! @author Paul Hermann Lensing
//! @brief epoll listener for asynchronous kinetic I/O
//------------------------------------------------------------------------------
#ifndef SOCKETLISTENER_HH
#define	SOCKETLISTENER_HH

/*----------------------------------------------------------------------------*/
#include <thread>
/*----------------------------------------------------------------------------*/

namespace kio{

//! Forward declaraction as AutoConnection includes this class.
class KineticAutoConnection;


//------------------------------------------------------------------------------
//! The SocketListener class spawns a background thread which uses epoll to
//! manage the file descriptors of registered kinetic auto connections.
//------------------------------------------------------------------------------
class SocketListener {
public:
  //----------------------------------------------------------------------------
  //! Subscribe the supplied KineticAutoConnection to the epoll listening
  //! thread. Throws if unsuccessful.
  //!
  //! @parameter fd the file descriptor to add to epoll
  //! @parameter connection the connection associated with the file descriptor
  //----------------------------------------------------------------------------
  void subscribe(int fd, KineticAutoConnection* connection);
  
  //----------------------------------------------------------------------------
  //! Remove the fd from the epoll listening thread. Never throws, if the fd
  //! was already removed automatically because it has been closed that's fine.
  //!
  //! @parameter fd the file descriptor to add to epoll
  //! @parameter connection the connection associated with the file descriptor
  //----------------------------------------------------------------------------
  void unsubscribe(int fd);

  //----------------------------------------------------------------------------
  //! Constructor.
  //----------------------------------------------------------------------------
  explicit SocketListener();

  //----------------------------------------------------------------------------
  //! Destructor.
  //----------------------------------------------------------------------------
  ~SocketListener();

private:
  //! thread object of epoll listener thread
  std::thread listener;

  //! indicate to the listener thread to shut down
  bool shutdown;

  //! the epoll fd
  int epoll_fd;

  //! uncopyable
  SocketListener (const SocketListener&) = delete;

  //! unassignable
  SocketListener& operator = (const SocketListener&) = delete;
};

}

#endif	/* SOCKETLISTENER_HH */


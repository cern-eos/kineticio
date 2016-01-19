//------------------------------------------------------------------------------
//! @file SocketListener.hh
//! @author Paul Hermann Lensing
//! @brief epoll listener for asynchronous kinetic I/O
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

#ifndef KINETICIO_SOCKETLISTENER_HH
#define	KINETICIO_SOCKETLISTENER_HH

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

#endif
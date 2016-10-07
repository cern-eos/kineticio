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

#include "SocketListener.hh"
#include "KineticAutoConnection.hh"
#include "Logging.hh"
#include <unistd.h>

#ifdef __APPLE__
#include <sys/event.h>
//#include <kqueue/sys/event.h>
#else
#include <sys/epoll.h>
#endif

using namespace kio;
using kinetic::KineticStatus;
using kinetic::StatusCode;

void listener_thread(int main_fd, bool* shutdown)
{
  fd_set a;
  int fd = 0;
  int max_events = 10;
#ifdef __APPLE__
  struct kevent events[max_events];
#else
  struct epoll_event events[max_events];
#endif

  while (1) {

#ifdef __APPLE__
    int ret = kevent(main_fd, NULL, 0, events, max_events, NULL);
#else
    int ret = epoll_wait(main_fd, events, max_events, -1);
#endif
    if (*shutdown) {
      break;
    }

    for (int i = 0; i < ret; i++) {

#ifdef __APPLE__
      auto con = (KineticAutoConnection*) events[i].udata;
#else
      auto con = (KineticAutoConnection*) events[i].data.ptr;
#endif
      if (con) {
        try {
          if (!con->get()->Run(&a, &a, &fd)) {
            throw std::runtime_error("Connection::Run(...) returned false");
          }
        } catch (const std::exception& e) {
          kio_warning("Error ", e.what(), " for ", con->getName());
        }
      }
      else{
        kio_notice("listener thread triggered but no connection available.");
      }
    }
  }
  kio_notice("listener thread exiting.");
}

SocketListener::SocketListener() :
    shutdown(false)
{
  /* Create the epoll / kqueue  descriptor. Only one is needed, and is used to monitor all sockets.*/
#ifdef __APPLE__
  listener_fd = kqueue();
#else
  listener_fd = epoll_create1(0);
#endif

  if (listener_fd < 0) {
    kio_error("Failed setting up fd listener");
    throw std::system_error(errno, std::generic_category());
  }
  kio_debug("set up listener_fd at ", listener_fd);

  listener = std::thread(listener_thread, listener_fd, &shutdown);
}

SocketListener::~SocketListener()
{
  kio_notice("entering destructor");
  int pipefd[2];
  pipe(pipefd);

#ifdef __APPLE__
  struct kevent e;
  EV_SET(&e, pipefd[0], EVFILT_READ, EV_ADD, 0, 0, NULL);
  kevent(listener_fd, &e, 1, NULL, 0, NULL);
#else
  struct epoll_event e;
  e.events = EPOLLIN | EPOLLOUT;
  e.data.ptr = NULL;
  epoll_ctl(listener_fd, EPOLL_CTL_ADD, pipefd[0], &e);
#endif

  shutdown = true;
  write(pipefd[1], "0", 1);

  listener.join();
  close(pipefd[0]);
  close(pipefd[1]);
  close(listener_fd);
  listener_fd=0;
  kio_notice("leaving destructor");
}



void SocketListener::subscribe(int fd, kio::KineticAutoConnection* connection)
{
  int rtn;
#ifdef __APPLE__
  struct kevent e[2];
  EV_SET(&e[0], fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, connection);
  EV_SET(&e[1], fd, EVFILT_READ, EV_ADD, 0, 0, connection);
  rtn = kevent(listener_fd, &e[1], 1, 0, 0, 0);
#else
  /* EPOLLIN: Ready to read
   * EPOLLOUT: Ready to write
   * EPOLLET: Edge triggered mode, only trigger when status changes. */
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
  ev.data.ptr = connection;

  /* Add the descriptor into the monitoring list. We can do it even if another
    thread is waiting in epoll_wait - the descriptor will be properly added */
  rtn = epoll_ctl(listener_fd, EPOLL_CTL_ADD, fd, &ev);
#endif
  if(rtn < 0){
    kio_error("failed adding fd ", fd, " to listener. ernno=", errno, " ", connection->getName());
    throw std::system_error(errno, std::generic_category());
  }
  kio_debug("Added fd ", fd, " for connection ", connection->getName(), " to listening queue.");
}

void SocketListener::unsubscribe(int fd)
{
  int rtn;
#ifdef __APPLE__
  struct kevent e[2];
  EV_SET(&e[0], fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
  EV_SET(&e[1], fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
  rtn = kevent(listener_fd, &e[1], 1, 0, 0, 0);
#else
  struct epoll_event ev;
  rtn = epoll_ctl(listener_fd, EPOLL_CTL_DEL, fd, &ev);
#endif
  if(rtn < 0) {
    kio_debug("failed to remove fd ", fd, " from listener. ernno=", errno);
  } else {
    kio_debug("succeessfully removed fd ", fd, " from listener.");
  }
}

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
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <unistd.h>

using namespace kio;
using kinetic::KineticStatus;
using kinetic::StatusCode;

void epoll_listen(int epoll_fd, bool* shutdown)
{
  int max_events = 10;
  struct epoll_event events[max_events];

  while (1) {
    int ret = epoll_wait(epoll_fd, events, max_events, -1);
    if (*shutdown) {
      break;
    }

    for (int i = 0; i < ret; i++) {
      auto con = (KineticAutoConnection*) events[i].data.ptr;
      fd_set a;
      int fd;

      if (con) {
        try {
          if (!con->get()->Run(&a, &a, &fd)) {
            throw std::runtime_error("Connection::Run(...) returned false");
          }
        } catch (const std::exception& e) {
          kio_warning("Error ", e.what(), " for ", con->getName());
          con->setError();
        }
      }
    }
  }
}

SocketListener::SocketListener() :
    shutdown(false)
{
  /* Create the epoll descriptor. Only one is needed, and is used to monitor all
   * sockets. */
  epoll_fd = epoll_create1(0);

  if (epoll_fd < 0) {
    kio_error("epoll_create failed");
    throw std::system_error(errno, std::generic_category());
  }

  listener = std::thread(epoll_listen, epoll_fd, &shutdown);
}

SocketListener::~SocketListener()
{
  int _eventfd = eventfd(0, EFD_NONBLOCK);
  struct epoll_event e = {EPOLLIN | EPOLLOUT, NULL};
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, _eventfd, &e);

  shutdown = true;
  eventfd_write(_eventfd, 1);
  listener.join();
  close(epoll_fd);
}


void SocketListener::subscribe(int fd, kio::KineticAutoConnection* connection)
{
  /* EPOLLIN: Ready to read
   * EPOLLOUT: Ready to write
   * EPOLLET: Edge triggered mode, only trigger when status changes. */
  struct epoll_event ev = {EPOLLIN | EPOLLOUT | EPOLLET, (void*) connection};

  /* Add the descriptor into the monitoring list. We can do it even if another
    thread is waiting in epoll_wait - the descriptor will be properly added */
  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) != 0) {
    kio_error("epoll_ctl add failed for fd ", fd, connection->getName());
    throw std::system_error(errno, std::generic_category());
  }
}

void SocketListener::unsubscribe(int fd)
{
  struct epoll_event ev;
  epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, &ev);
}

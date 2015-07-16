#include "SocketListener.hh"
#include "KineticAutoConnection.hh"
#include "LoggingException.hh"
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <thread>

using namespace kio;
using kinetic::KineticStatus;
using kinetic::StatusCode;

void epoll_listen(int epoll_fd, bool* shutdown)
{
  int max_events = 10;
  struct epoll_event events[ max_events ];

  while(1){
    int ret = epoll_wait(epoll_fd, events, max_events, -1);
    if(*shutdown)
      break;

    for (int i=0; i<ret; i++){
      auto con = (KineticAutoConnection*) events[i].data.ptr;
      fd_set a; int fd;

      try{
        if(!con->get()->Run(&a,&a,&fd))
          throw std::runtime_error("Connection Failed");
      }catch(const std::exception& e){
        con->setError( KineticStatus(StatusCode::CLIENT_IO_ERROR,e.what()) );
      }
//      printf("event %d of %d is of type %s: run on pointer %p returned fd %d\n",
//        i,ret,
//        events[i].events & EPOLLIN ? "EPOLLIN" : events[i].events & EPOLLOUT ? "EPOLLOUT" : "UNKNOWN",
//        con, fd ? fd-1 : 0);

    }
  }
}

SocketListener::SocketListener() :
shutdown(false)
{
  /* Create the epoll descriptor. Only one is needed, and is used to monitor all
   * sockets. */
  epoll_fd = epoll_create1(0);

  if ( epoll_fd < 0 ){
    throw LoggingException(errno,__FUNCTION__,__FILE__,__LINE__,
          "epoll_create failed."
          );
  }
  listener = std::thread(epoll_listen, epoll_fd, &shutdown);
}

SocketListener::~SocketListener()
{
  shutdown=true;
  int _eventfd = eventfd(0, EFD_NONBLOCK);
  struct epoll_event e;
  e.events = EPOLLIN | EPOLLOUT;
  epoll_ctl( epoll_fd, EPOLL_CTL_ADD, _eventfd, &e);
  eventfd_write(_eventfd, 1);
  listener.join();
  close(epoll_fd);
}


void SocketListener::subscribe(int fd, kio::KineticAutoConnection* connection)
{
  /* EPOLLIN: Ready to read
   * EPOLLOUT: Ready to write
   * EPOLLET: Edge triggered mode, only trigger when status changes. */
  struct epoll_event ev= {EPOLLIN | EPOLLOUT | EPOLLET, (void*) connection};

  /* Add the descriptor into the monitoring list. We can do it even if another
    thread is waiting in epoll_wait - the descriptor will be properly added */
  if (epoll_ctl( epoll_fd, EPOLL_CTL_ADD, fd, &ev) != 0 )
    throw LoggingException(errno,__FUNCTION__,__FILE__,__LINE__,
          "epoll_ctl add failed."
          );
}

void SocketListener::unsubscribe(int fd)
{
  struct epoll_event ev;
  epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, &ev);
}

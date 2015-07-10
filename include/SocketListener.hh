#ifndef SOCKETLISTENER_HH
#define	SOCKETLISTENER_HH

#include <thread>

namespace kio{

/* Forward declaraction as AutoConnection includes this class. */
class KineticAutoConnection;

class SocketListener {
public:
  void subscribe(int fd, KineticAutoConnection* connection);
  void unsubscribe(int fd);

  explicit SocketListener();
  ~SocketListener();

private:
  std::thread listener;
  bool shutdown;
  int epoll_fd;
 
  SocketListener (const SocketListener&) = delete;
  SocketListener& operator = (const SocketListener&) = delete;
};

}

#endif	/* SOCKETLISTENER_HH */


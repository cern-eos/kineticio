#include "KineticAutoConnection.hh"
#include "LoggingException.hh"
#include <sstream>

using namespace kinetic;
using namespace kio;

KineticAutoConnection::KineticAutoConnection(
    SocketListener &sw,
    std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions> o,
    std::chrono::seconds r) :
    connection(), fd(0), options(o), timestamp(), ratelimit(r),
    status(kinetic::StatusCode::CLIENT_INTERNAL_ERROR, ""),
    mutex(), sockwatch(sw), mt()
{
  std::random_device rd;
  mt.seed(rd());
}

KineticAutoConnection::~KineticAutoConnection()
{
}

void KineticAutoConnection::setError(kinetic::KineticStatus s)
{
  std::lock_guard<std::mutex> lck(mutex);
  status = s;
  if (fd) {
    sockwatch.unsubscribe(fd);
    fd = 0;
  }
}

std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection> KineticAutoConnection::get()
{
  std::lock_guard<std::mutex> lck(mutex);

  if (!status.ok()) {
    connect();
    if (!status.ok())
      throw LoggingException(ENXIO, __FUNCTION__, __FILE__, __LINE__,
                             "Invalid connection: " + status.message());
  }
  return connection;
}

class ConnectCallback : public kinetic::SimpleCallbackInterface
{
public:
  void Success() { }

  void Failure(kinetic::KineticStatus error) { }

  ConnectCallback() { }

  ~ConnectCallback() { }
};

void KineticAutoConnection::connect()
{
  /* Rate limit connection attempts. */
  using std::chrono::system_clock;
  using std::chrono::duration_cast;
  using std::chrono::seconds;
  if (duration_cast<seconds>(system_clock::now() - timestamp) < ratelimit)
    return;

  /* Remember this reconnection attempt. */
  timestamp = system_clock::now();

  /* Choose connection to prioritize at random. */
  int r = mt() % 2;
  auto &primary = r ? options.first : options.second;
  auto &secondary = r ? options.second : options.first;

  KineticConnectionFactory factory = NewKineticConnectionFactory();
  if (factory.NewThreadsafeNonblockingConnection(primary, connection).ok() ||
      factory.NewThreadsafeNonblockingConnection(secondary, connection).ok()) {
    auto cb = std::make_shared<ConnectCallback>();
    fd_set a;
    connection->NoOp(cb);
    connection->Run(&a, &a, &fd);
    if (fd) {
      fd--;
      sockwatch.subscribe(fd, this);
      status = KineticStatus(StatusCode::OK, "");
      return;
    }
  }
  std::stringstream ss;
  ss << "Failed building connection to " << options.first.host << ":"
  << options.first.port << " and " << options.second.host << ":"
  << options.second.port;
  status = KineticStatus(StatusCode::REMOTE_REMOTE_CONNECTION_ERROR, ss.str());
}

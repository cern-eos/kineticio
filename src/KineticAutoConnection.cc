#include "KineticAutoConnection.hh"
#include "LoggingException.hh"
#include <sstream>
#include <Logging.hh>

using namespace kinetic;
using namespace kio;

KineticAutoConnection::KineticAutoConnection(
    SocketListener &sw,
    std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions> o,
    std::chrono::seconds r) :
    connection(), fd(0), options(o), timestamp(), ratelimit(r),
    status(kinetic::StatusCode::CLIENT_INTERNAL_ERROR, "No Connection attempt yet."),
    mutex(), bg(1), sockwatch(sw), mt()
{
  std::random_device rd;
  mt.seed(rd());
}

KineticAutoConnection::~KineticAutoConnection()
{
  if (fd) {
    sockwatch.unsubscribe(fd);
  }
}

void KineticAutoConnection::setError(kinetic::KineticStatus s)
{
  std::lock_guard<std::mutex> lock(mutex);
  status = s;
  if (fd) {
    sockwatch.unsubscribe(fd);
    fd = 0;
  }
}

std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection> KineticAutoConnection::get()
{
  std::call_once(intial_connect, &KineticAutoConnection::connect, this);

  std::lock_guard<std::mutex> lock(mutex);
  if (status.ok())
    return connection;

  /* Rate limit connection attempts. */
  using std::chrono::system_clock;
  using std::chrono::duration_cast;
  using std::chrono::seconds;
  if (duration_cast<seconds>(system_clock::now() - timestamp) > ratelimit) {
    auto function = std::bind(&KineticAutoConnection::connect, this);
    bg.try_run(function);
  }
  throw kio_exception(ENXIO, "Invalid connection: ", status.message());
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
  /* Choose connection to prioritize at random. */
  auto r = mt() % 2;
  auto& primary = r ? options.first : options.second;
  auto& secondary = r ? options.second : options.first;

  auto tmpfd = 0;
  std::shared_ptr<ThreadsafeNonblockingKineticConnection> tmpcon;
  KineticConnectionFactory factory = NewKineticConnectionFactory();

  if (factory.NewThreadsafeNonblockingConnection(primary, tmpcon).ok() ||
      factory.NewThreadsafeNonblockingConnection(secondary, tmpcon).ok()) {
    auto cb = std::make_shared<ConnectCallback>();
    fd_set a;
    tmpcon->NoOp(cb);
    tmpcon->Run(&a, &a, &tmpfd);
  }
  {
    std::lock_guard<std::mutex> lock(mutex);
    timestamp = std::chrono::system_clock::now();
    if (tmpfd) {
      tmpfd--;
      try{
        sockwatch.subscribe(tmpfd, this);
      }catch(const std::exception& e){
        status=KineticStatus(StatusCode::CLIENT_IO_ERROR, e.what());
        throw e;
      }
      fd = tmpfd;
      status = KineticStatus(StatusCode::OK, "");
      connection = std::move(tmpcon);
    }
    else {
      std::stringstream ss;
      ss << "Failed building connection to " << options.first.host << ":"
      << options.first.port << " and " << options.second.host << ":"
      << options.second.port;
      status = KineticStatus(StatusCode::CLIENT_IO_ERROR, ss.str());
    }
  }
}

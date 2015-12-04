#include "KineticAutoConnection.hh"
#include "LoggingException.hh"
#include "KineticCallbacks.hh"
#include "KineticIoSingleton.hh"
#include <sstream>
#include <Logging.hh>

using namespace kinetic;
using namespace kio;

KineticAutoConnection::KineticAutoConnection(
    SocketListener &sw,
    std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions> o,
    std::chrono::seconds r) :
    connection(), healthy(false), fd(0), options(o), timestamp(), ratelimit(r),
    mutex(), dmutex(std::make_shared<DestructionMutex>()), sockwatch(sw), mt()
{
  std::random_device rd;
  mt.seed(rd());
  logstring = utility::Convert::toString(
      "(",options.first.host, ":", options.first.port, " / ", options.second.host, ":", options.second.port,")"
  );
}

KineticAutoConnection::~KineticAutoConnection()
{
  dmutex->setDestructed();
  if (fd) {
    sockwatch.unsubscribe(fd);
  }
}

const std::string& KineticAutoConnection::getName()
{
  return logstring;
}

void KineticAutoConnection::setError()
{
  std::lock_guard<std::mutex> lock(mutex);
  if (fd) {
    sockwatch.unsubscribe(fd);
    fd = 0;
  }
  healthy = false;
}

std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection> KineticAutoConnection::get()
{
  std::call_once(intial_connect, &KineticAutoConnection::connect, this, dmutex);

  std::lock_guard<std::mutex> lock(mutex);
  if (healthy)
    return connection;

  /* Rate limit connection attempts. */
  using namespace std::chrono;
  auto duration = duration_cast<seconds>(system_clock::now() - timestamp);
  if (duration > ratelimit) {
    if(kio().threadpool().try_run(std::bind(&KineticAutoConnection::connect, this, dmutex))) {
      timestamp = std::chrono::system_clock::now();
      kio_debug("Attempting background reconnect. Last reconnect attempt has been ",
                duration, " seconds ago. ratelimit is ", ratelimit, " seconds ", logstring);
    }
  }
  throw kio_exception(ENXIO, "No valid connection ", logstring);
}

namespace{
  class ConnectCallback : public kinetic::SimpleCallbackInterface
  {
  public:
    void Success() { _done = _success = true; }
    void Failure(kinetic::KineticStatus error) { _done = true; }  
    ConnectCallback() : _done(false), _success(false) { }
    ~ConnectCallback() { }
    bool done(){ return _done; }
    bool ok(){ return _success; }

  private: 
    bool _done; 
    bool _success;
  };
}

void KineticAutoConnection::connect( std::shared_ptr<DestructionMutex> protect)
{
  std::lock_guard<DestructionMutex> dlock(*protect);
  
  /* Choose connection to prioritize at random. */
  auto r = mt() % 2;
  auto& primary = r ? options.first : options.second;
  auto& secondary = r ? options.second : options.first;

  auto tmpfd = 0;
  std::shared_ptr<ThreadsafeNonblockingKineticConnection> tmpcon;
  KineticConnectionFactory factory = NewKineticConnectionFactory();
  
  if (factory.NewThreadsafeNonblockingConnection(primary, tmpcon).ok() ||
      factory.NewThreadsafeNonblockingConnection(secondary, tmpcon).ok()) 
  {
    auto cb = std::make_shared<ConnectCallback>();    
    tmpcon->NoOp(cb);
    
    /* Get the fd */
    fd_set x; int y; 
    tmpcon->Run(&x, &x, &tmpfd);
    
    /* wait on noop reply... we actually don't want to add drives where the connection succeeds 
     * but requests don't (e.g. drive is in locked state or has an error) */  
    while(tmpcon->Run(&x, &x, &y) && !cb->done()) { 
      struct timeval tv{5,0};
      if( select(tmpfd, &x, &x, NULL, &tv) <= 0 )
        break;
    }
    if(!cb->ok())
      tmpfd = 0; 
  }
  
  {
    std::lock_guard<std::mutex> lock(mutex);
    if (tmpfd) {
      tmpfd--;
      try{
        sockwatch.subscribe(tmpfd, this);
      }catch(const std::exception& e){
        kio_warning(e.what());
        throw e;
      }
      fd = tmpfd;
      connection = std::move(tmpcon);
      healthy = true;
      kio_debug("connection attempt succeeded ", logstring);
    }
    else {
      kio_debug("Connection attempt failed ", logstring);
    }
  }
}

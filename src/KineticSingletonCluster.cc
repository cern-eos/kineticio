#include "KineticSingletonCluster.hh"
#include "KineticException.hh"
#include <uuid/uuid.h>
#include <functional>
#include <thread>

using std::chrono::system_clock;
using std::unique_ptr;
using std::shared_ptr;
using std::string;
using namespace kinetic;
using com::seagate::kinetic::client::proto::Command_Algorithm_SHA1;

KineticSingletonCluster::KineticSingletonCluster(
    const kinetic::ConnectionOptions &ci,
    std::chrono::seconds connect_min,
    std::chrono::seconds getlog_min) :
  con(), connection_info(ci), mutex(), cluster_limits{0,0,0,0,0,0,0,0}, drive_log(),
  getlog_timestamp(), getlog_ratelimit(getlog_min),
  getlog_status(StatusCode::CLIENT_SHUTDOWN,""),
  connection_timestamp(), connection_ratelimit(connect_min),
  connection_status(StatusCode::CLIENT_SHUTDOWN,"")
{
  connect();
  if(!connection_status.ok())
    throw std::runtime_error("Initial connection failed: "+connection_status.message());
  getLog();
  if(!getlog_status.ok())
    throw std::runtime_error("Initial getlog operation failed: "+getlog_status.message());
}

KineticSingletonCluster::~KineticSingletonCluster()
{
}

void KineticSingletonCluster::connect()
{
  std::lock_guard<std::mutex> lck(mutex);

  /* Rate limit connection attempts. */
  if(std::chrono::duration_cast<std::chrono::milliseconds>
    (system_clock::now() - connection_timestamp) < connection_ratelimit)
    return;

  /* Remember this reconnection attempt. */
  connection_timestamp = system_clock::now();

  /* If NoOp succeeds -> false alarm. */
  if(con && con->NoOp().ok()){
    connection_status = KineticStatus(StatusCode::OK,"");
    return;
  }

  KineticConnectionFactory factory = NewKineticConnectionFactory();
  if(factory.NewThreadsafeBlockingConnection(connection_info, con, 10).ok()){
    connection_status = KineticStatus(StatusCode::OK,"");
  }
  else{
    connection_status = KineticStatus(StatusCode::REMOTE_REMOTE_CONNECTION_ERROR,
            "Failed building connection to "
            +connection_info.host+":"
            +std::to_string((long long int)connection_info.port));
  }
}

KineticStatus KineticSingletonCluster::execute(std::function<KineticStatus(void)> fun)
{
  for(int attempt=0; attempt<2; attempt++){
    if(!connection_status.ok())
      connect();
    if(connection_status.ok()){
      auto status = fun();

      if( status.statusCode() == StatusCode::REMOTE_REMOTE_CONNECTION_ERROR ||
          status.statusCode() == StatusCode::REMOTE_INTERNAL_ERROR ||
          status.statusCode() == StatusCode::CLIENT_INTERNAL_ERROR ||
          status.statusCode() == StatusCode::CLIENT_SHUTDOWN ||
          status.statusCode() == StatusCode::CLIENT_IO_ERROR){
        connection_status = status;
      } else{
        return status;
      }
    }
  }
  return connection_status;
}

KineticStatus KineticSingletonCluster::get(
                  const shared_ptr<const string>& key,
                  bool skip_value,
                  shared_ptr<const string>& version,
                  shared_ptr<const string>& value)
{
  if(skip_value){
    unique_ptr<string> v;
    auto f = std::bind<KineticStatus(ThreadsafeBlockingKineticConnection::*)(
            const shared_ptr<const string>,
            unique_ptr<string>&)>(
        &ThreadsafeBlockingKineticConnection::GetVersion, std::ref(con),
            std::cref(key),
            std::ref(v)
    );
    auto status = execute(f);

    if(status.ok())
      version.reset(new string(std::move(*v)));
    return status;
  }

  unique_ptr<KineticRecord> record;
  auto f = std::bind<KineticStatus(ThreadsafeBlockingKineticConnection::*)(
          const shared_ptr<const string>,
          unique_ptr<KineticRecord>&)>(
    &ThreadsafeBlockingKineticConnection::Get, std::ref(con),
          std::cref(key),
          std::ref(record)
  );
  auto status = execute(f);
  if(status.ok()){
    version = record->version();
    value = record->value();
  }
  return status;
}

KineticStatus KineticSingletonCluster::put(
    const shared_ptr<const string>& key,
    const shared_ptr<const string>& version_in,
    const shared_ptr<const string>& value,
    bool force,
    shared_ptr<const string>& version_out)
{
  /* Generate new UUID as version... do not store directly in version_out
   * in case client uses the same variable for version_in and version_out. */
  uuid_t uuid;
  uuid_generate(uuid);
  auto version_new = std::make_shared<string>(
    string(reinterpret_cast<const char *>(uuid), sizeof(uuid_t)));

  /* Generate SHA1 tag. TODO */
  std::shared_ptr<string> tag = std::make_shared<string>("");

  /* Construct record structure. */
  shared_ptr<KineticRecord> record(
          new KineticRecord(value, version_new, tag, Command_Algorithm_SHA1)
  );
  
  auto f =  std::bind<KineticStatus(ThreadsafeBlockingKineticConnection::*)(
              const shared_ptr<const string>,
              const shared_ptr<const string>,
              WriteMode,
              const shared_ptr<const KineticRecord>,
              PersistMode)>(
    &ThreadsafeBlockingKineticConnection::Put, std::ref(con),
        std::cref(key),
        version_in ? version_in : make_shared<const string>(),
        force ? WriteMode::IGNORE_VERSION : WriteMode::REQUIRE_SAME_VERSION,
        std::cref(record),
        PersistMode::WRITE_BACK
  );

  auto status = execute(f);
  version_out = version_new;
  return status;
}

KineticStatus KineticSingletonCluster::remove(
                     const shared_ptr<const string>& key,
                     const shared_ptr<const string>& version,
                     bool force)
{
  auto f = std::bind<KineticStatus(ThreadsafeBlockingKineticConnection::*)(
            const shared_ptr<const string>,
            const shared_ptr<const string>,
            WriteMode,
            PersistMode)>(
    &ThreadsafeBlockingKineticConnection::Delete, std::ref(con),
        std::cref(key),
        std::cref(version),
        force ? WriteMode::IGNORE_VERSION : WriteMode::REQUIRE_SAME_VERSION,
        PersistMode::WRITE_BACK
  );
  return execute(f);
}

KineticStatus KineticSingletonCluster::range(
          const std::shared_ptr<const std::string>& start_key,
          const std::shared_ptr<const std::string>& end_key,
          int maxRequested,
          std::unique_ptr< std::vector<std::string> >& keys)
{
  auto f = std::bind<KineticStatus(ThreadsafeBlockingKineticConnection::*)(
            const shared_ptr<const string>, bool,
            const shared_ptr<const string>, bool,
            bool,
            int32_t,
            unique_ptr<vector<string>>&)>(
    &ThreadsafeBlockingKineticConnection::GetKeyRange, std::ref(con),
        std::cref(start_key), true,
        std::cref(end_key), true,
        false,
        maxRequested,
        std::ref(keys)
  );
  return execute(f);
}

KineticStatus KineticSingletonCluster::getLog()
{
  unique_ptr<kinetic::DriveLog> tmp_log;

  std::vector<Command_GetLog_Type> types = {
    Command_GetLog_Type::Command_GetLog_Type_CAPACITIES,
    Command_GetLog_Type::Command_GetLog_Type_UTILIZATIONS
  };
  if(!drive_log)
    types.push_back(Command_GetLog_Type::Command_GetLog_Type_LIMITS);

  auto f = std::bind<KineticStatus(ThreadsafeBlockingKineticConnection::*)(
            const vector<Command_GetLog_Type>&,
            unique_ptr<DriveLog>&)>(
    &ThreadsafeBlockingKineticConnection::GetLog, std::ref(con),
        std::cref(types),
        std::ref(tmp_log)
  );
  auto status = execute(f);

  std::lock_guard<std::mutex> lck(mutex);
  getlog_status = status;
  getlog_timestamp = system_clock::now();

  if(status.ok()){
    if(!drive_log) cluster_limits = tmp_log->limits;
    std::swap(drive_log,tmp_log);
  }
  return status;
}

const Limits& KineticSingletonCluster::limits() const
{
  return cluster_limits;
}

KineticStatus KineticSingletonCluster::size(kinetic::Capacity& size)
{
  std::lock_guard<std::mutex> lck(mutex);
  if(std::chrono::duration_cast<std::chrono::milliseconds>
    (system_clock::now() - getlog_timestamp) > getlog_ratelimit){
    getlog_timestamp = system_clock::now();
    std::thread(&KineticSingletonCluster::getLog, this).detach();
  }
  if(getlog_status.ok())
    size = drive_log->capacity;
  return getlog_status;
}
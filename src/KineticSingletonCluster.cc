#include "KineticSingletonCluster.hh"
#include "LoggingException.hh"
#include <uuid/uuid.h>
#include <zlib.h>
#include <functional>
#include <algorithm>
#include <thread>

using std::chrono::system_clock;
using std::unique_ptr;
using std::shared_ptr;
using std::string;
using namespace kinetic;

KineticSingletonCluster::KineticSingletonCluster(
    const kinetic::ConnectionOptions &ci,
    std::chrono::seconds connect_min,
    std::chrono::seconds getlog_min) :
  con(), connection_info(ci), mutex(), clusterlimits{0,0,0}, clustersize{0,0},
  getlog_timestamp(), getlog_ratelimit(getlog_min),
  getlog_status(StatusCode::CLIENT_SHUTDOWN,""),
  connection_timestamp(), connection_ratelimit(connect_min),
  connection_status(StatusCode::CLIENT_SHUTDOWN,"")
{
  connect();
  if(!connection_status.ok())
    throw LoggingException(ENXIO,__FUNCTION__,__FILE__,__LINE__,
            "Initial connection failed: "+connection_status.message());
  getLog({Command_GetLog_Type::Command_GetLog_Type_LIMITS,
          Command_GetLog_Type::Command_GetLog_Type_CAPACITIES});
  if(!getlog_status.ok())
    throw LoggingException(ENXIO,__FUNCTION__,__FILE__,__LINE__,
            "Initial getlog failed: "+getlog_status.message());
}

KineticSingletonCluster::~KineticSingletonCluster()
{
}

void KineticSingletonCluster::connect()
{
  std::lock_guard<std::mutex> lck(mutex);

  /* Rate limit connection attempts. */
  using std::chrono::duration_cast;
  using std::chrono::seconds;
  if(duration_cast<seconds>(system_clock::now() - connection_timestamp) < connection_ratelimit)
    return;

  /* If NoOp succeeds -> false alarm. */
  if(con && con->NoOp().ok()){
    connection_status = KineticStatus(StatusCode::OK,"");
    return;
  }

  /* Remember this reconnection attempt. */
  connection_timestamp = system_clock::now();

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
    if(!connection_status.ok()){
      connect();
    }
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
    reinterpret_cast<const char *>(uuid), sizeof(uuid_t)
  );

  /* Generate Checksum. */
  auto checksum = value ? 0 : crc32(0, (const Bytef*) value->c_str(), value->length());
  auto tag = std::make_shared<string>(
      std::to_string((long long unsigned int) checksum)
  );

  /* Construct record structure. */
  shared_ptr<KineticRecord> record(
    new KineticRecord(value, version_new, tag,
      com::seagate::kinetic::client::proto::Command_Algorithm_CRC32)
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

KineticStatus KineticSingletonCluster::getLog(std::vector<Command_GetLog_Type> types)
{
  unique_ptr<kinetic::DriveLog> drive_log;
  auto f = std::bind<KineticStatus(ThreadsafeBlockingKineticConnection::*)(
            const vector<Command_GetLog_Type>&,
            unique_ptr<DriveLog>&)>(
    &ThreadsafeBlockingKineticConnection::GetLog, std::ref(con),
        std::cref(types),
        std::ref(drive_log)
  );
  auto status = execute(f);

  std::lock_guard<std::mutex> lck(mutex);
  getlog_status = status;
  getlog_timestamp = system_clock::now();

  if(status.ok()){
    if(std::find(types.begin(), types.end(),
        Command_GetLog_Type::Command_GetLog_Type_CAPACITIES) != types.end()){
      const auto& c = drive_log->capacity;
      clustersize.bytes_total = c.nominal_capacity_in_bytes;
      clustersize.bytes_free  = c.nominal_capacity_in_bytes -
                          (c.nominal_capacity_in_bytes * c.portion_full);
    }
    if(std::find(types.begin(), types.end(),
        Command_GetLog_Type::Command_GetLog_Type_LIMITS) != types.end()){
      const auto& l = drive_log->limits;
      clusterlimits.max_key_size = l.max_key_size;
      clusterlimits.max_value_size = l.max_value_size;
      clusterlimits.max_version_size = l.max_version_size;
    }
  }
  return status;
}

const KineticClusterLimits& KineticSingletonCluster::limits() const
{
  return clusterlimits;
}

KineticStatus KineticSingletonCluster::size(KineticClusterSize& size)
{
  std::lock_guard<std::mutex> lck(mutex);

  /* rate limit getlog requests */
  using std::chrono::duration_cast;
  using std::chrono::seconds;

  if(duration_cast<seconds>(system_clock::now() - getlog_timestamp) > getlog_ratelimit){

    getlog_timestamp = system_clock::now();
    std::vector<Command_GetLog_Type> v =
        {Command_GetLog_Type::Command_GetLog_Type_CAPACITIES};

    std::thread(&KineticSingletonCluster::getLog, this, v).detach();
  }

  if(getlog_status.ok())
    size = size;
  return getlog_status;
}
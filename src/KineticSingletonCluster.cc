#include "KineticSingletonCluster.hh"
#include <uuid/uuid.h>

using std::chrono::system_clock;
using std::unique_ptr;
using std::shared_ptr;
using std::string;
using namespace kinetic;
using com::seagate::kinetic::client::proto::Command_Algorithm_SHA1;

KineticSingletonCluster::KineticSingletonCluster(const kinetic::ConnectionOptions &ci) :
  connection_info(ci), con(), cluster_limits{0,0,0,0,0,0,0,0}, cluster_size{0,0},
  size_timepoint(), size_expiration(5000)
{
  connect();
}

KineticSingletonCluster::~KineticSingletonCluster()
{
}

kinetic::KineticStatus KineticSingletonCluster::connect()
{
  KineticConnectionFactory factory = NewKineticConnectionFactory();
  if(factory.NewThreadsafeBlockingConnection(connection_info, con, 30).notOk())
     return KineticStatus(StatusCode::REMOTE_REMOTE_CONNECTION_ERROR,
             "Failed building connection");

  /* Initialize limits and sizes. */
  std::vector<Command_GetLog_Type> types = {
    Command_GetLog_Type::Command_GetLog_Type_LIMITS,
    Command_GetLog_Type::Command_GetLog_Type_CAPACITIES
  };
  unique_ptr<DriveLog> log;

  if(con->GetLog(types, log).ok()){
    cluster_limits = log->limits;
    cluster_size = log->capacity;
    size_timepoint = system_clock::now();
  }

  return KineticStatus(StatusCode::OK,"");
}

bool KineticSingletonCluster::ok()
{
  if(!con && !connect().ok())
      return false;
  return con->NoOp().ok();
}

const kinetic::Limits& KineticSingletonCluster::limits()
{
  return cluster_limits;
}

kinetic::Capacity KineticSingletonCluster::size()
{
  if(con && std::chrono::duration_cast<std::chrono::milliseconds>
          (system_clock::now() - size_timepoint) > size_expiration){
    std::vector<Command_GetLog_Type> types = {
      Command_GetLog_Type::Command_GetLog_Type_CAPACITIES
    };
    unique_ptr<DriveLog> log;

    if(con->GetLog(types, log).ok()){
      cluster_size = log->capacity;
      size_timepoint = system_clock::now();
    }
  }

  return cluster_size;
}

KineticStatus KineticSingletonCluster::get(
                  const shared_ptr<const string>& key,
                  bool skip_value,
                  shared_ptr<const string>& version,
                  shared_ptr<const string>& value)
{
  if(skip_value){
    unique_ptr<string> v;
    KineticStatus status = con->GetVersion(key, v);
      if(status.ok())
        version.reset(new string(std::move(*v)));
    return status;
  }

  unique_ptr<KineticRecord> record;
  KineticStatus status = con->Get(key, record);
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

  auto status = con->Put(key,
          version_in ? version_in : make_shared<const string>(),
          force ? WriteMode::IGNORE_VERSION : WriteMode::REQUIRE_SAME_VERSION,
          record, PersistMode::WRITE_BACK);
  version_out = version_new;
  return status;
}

KineticStatus KineticSingletonCluster::remove(
                     const shared_ptr<const string>& key,
                     const shared_ptr<const string>& version,
                     bool force)
{
  return con->Delete(key, version,
          force ? WriteMode::IGNORE_VERSION : WriteMode::REQUIRE_SAME_VERSION,
          PersistMode::WRITE_BACK);
}

KineticStatus KineticSingletonCluster::range(
          const std::shared_ptr<const std::string>& start_key,
          const std::shared_ptr<const std::string>& end_key,
          int maxRequested,
          std::unique_ptr< std::vector<std::string> >& keys)
{
  return con->GetKeyRange(start_key, true, end_key, true, false,
          maxRequested, keys);
}
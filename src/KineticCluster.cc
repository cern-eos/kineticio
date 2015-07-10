#include "KineticCluster.hh"
#include "LoggingException.hh"
#include "ErasureCoding.hh"
#include "Utility.hh"
#include <uuid/uuid.h>
#include <zlib.h>
#include <functional>
#include <algorithm>
#include <thread>
#include <set>
#include <condition_variable>

using std::chrono::system_clock;
using std::unique_ptr;
using std::shared_ptr;
using std::string;
using namespace kinetic;
using namespace kio;


class GetCallback : public KineticCallback, public GetCallbackInterface{
public:
  explicit GetCallback(shared_ptr<KineticOperationSync>& s):KineticCallback(s){};
  ~GetCallback(){};

  void Success(const std::string &key, std::unique_ptr<KineticRecord> r){
    record = std::move(r);
    OnResult(KineticStatus(StatusCode::OK, ""));
  }
  void Failure(KineticStatus error){
    OnResult(error);
  };

  const std::unique_ptr<KineticRecord>& getRecord(){
    return record;
  }

private:
  std::unique_ptr<KineticRecord> record;
};

class GetVersionCallback : public KineticCallback, public GetVersionCallbackInterface{
public:
  explicit GetVersionCallback(shared_ptr<KineticOperationSync>& s):KineticCallback(s){};
  ~GetVersionCallback(){};

  void Success(const std::string &v){
    version = v;
    OnResult(KineticStatus(StatusCode::OK, ""));
  }
  void Failure(KineticStatus error){
    OnResult(error);
  };

  const std::string& getVersion(){
    return version;
  }

private:
   std::string version;

};

class GetLogCallback : public KineticCallback, public GetLogCallbackInterface{
public:
  explicit GetLogCallback(shared_ptr<KineticOperationSync>& s):KineticCallback(s){};
  ~GetLogCallback() {};

  void Success(unique_ptr<DriveLog> dlog){
    drive_log = std::move(dlog);
    OnResult(KineticStatus(StatusCode::OK, ""));
  }
  void Failure(KineticStatus error){
    OnResult(error);
  };

  unique_ptr<DriveLog>& getLog(){
    return drive_log;
  }
private:
    unique_ptr<DriveLog> drive_log;
};

class PutCallback : public KineticCallback, public PutCallbackInterface{
public:
  explicit PutCallback(shared_ptr<KineticOperationSync>& s):KineticCallback(s){};
  ~PutCallback() {};

  void Success(){
    OnResult(KineticStatus(StatusCode::OK, ""));
  }
  void Failure(KineticStatus error){
    OnResult(error);
  };
};

class DeleteCallback : public KineticCallback, public SimpleCallbackInterface{
public:
  explicit DeleteCallback(shared_ptr<KineticOperationSync>& s):KineticCallback(s){};
  ~DeleteCallback() {};

  void Success(){
    OnResult(KineticStatus(StatusCode::OK, ""));
  }
  void Failure(KineticStatus error){
    OnResult(error);
  };
};

class RangeCallback : public KineticCallback, public GetKeyRangeCallbackInterface{
public:
  explicit RangeCallback(shared_ptr<KineticOperationSync>& s):KineticCallback(s){};
  ~RangeCallback() {};

  void Success(unique_ptr<vector<string>> k){
    keys = std::move(k);
    OnResult(KineticStatus(StatusCode::OK, ""));
  }
  void Failure(KineticStatus error){
    OnResult(error);
  };

  unique_ptr< vector<string> >& getKeys(){
    return keys;
  }
private:
    unique_ptr< vector<string> > keys;
};


KineticCluster::KineticCluster(
    std::size_t stripe_size, std::size_t num_parities,
    std::vector< std::pair < kinetic::ConnectionOptions, kinetic::ConnectionOptions > > info,
    std::chrono::seconds min_reconnect_interval,
    std::chrono::seconds op_timeout,
    std::shared_ptr<ErasureCoding> ec, 
    SocketListener& listener
) :
    nData(stripe_size), nParity(num_parities),
    connections(), operation_timeout(op_timeout),
    clusterlimits{0,0,0}, clustersize{0,0},
    getlog_status(StatusCode::CLIENT_INTERNAL_ERROR,"not initialized"),
    getlog_outstanding(false),
    getlog_mutex(), erasure(ec)
{
  if(nData+nParity > info.size())
    throw std::logic_error("Stripe size + parity size cannot exceed cluster size.");

  for(auto i = info.begin(); i != info.end(); i++){
    auto ncon = KineticAutoConnection(listener, *i, min_reconnect_interval);
    connections.push_back(ncon);
  }

  if(!getLog({
      Command_GetLog_Type::Command_GetLog_Type_LIMITS,
      Command_GetLog_Type::Command_GetLog_Type_CAPACITIES
    }).ok()
   ) throw LoggingException(
            ENXIO,__FUNCTION__,__FILE__,__LINE__,
            "Initial getlog failed: " + getlog_status.message()
          );
    clusterlimits.max_value_size*=nData;
}

KineticCluster::~KineticCluster()
{
  /* Ensure that no background getlog operation is running, as it will
     access member variables. */
  while(true){
    std::lock_guard<std::mutex> lck(getlog_mutex);
    if(getlog_outstanding==false)
      break;
  };
}

/* return the frequency of the most common element in the vector, as well
   as a reference to that element. */
KineticAsyncOperation& mostFrequent(
    std::vector<KineticAsyncOperation>& ops, int& count,
    std::function<bool(const KineticAsyncOperation& ,const KineticAsyncOperation&)> equal
)
{
  count = 0;
  auto element = ops.begin();

  for(auto o = ops.begin(); o != ops.end(); o++){
    int frequency = 0;
    for(auto l = ops.begin(); l!= ops.end(); l++)
      if(equal(*o,*l))
        frequency++;

    if(frequency > count){
      count=frequency;
      element=o;
    }
    if(frequency > ops.size()/2)
      break;
  }
  return *element;
}

bool resultsEqual(const KineticAsyncOperation& lhs ,const KineticAsyncOperation& rhs)
{
  return lhs.callback->getResult().statusCode() == rhs.callback->getResult().statusCode();
}

bool getVersionEqual(const KineticAsyncOperation& lhs ,const KineticAsyncOperation& rhs)
{
  return
    std::static_pointer_cast<GetVersionCallback>(lhs.callback)->getVersion()
          ==
    std::static_pointer_cast<GetVersionCallback>(rhs.callback)->getVersion();
}
bool getRecordVersionEqual(const KineticAsyncOperation& lhs ,const KineticAsyncOperation& rhs)
{
  return
    * std::static_pointer_cast<GetCallback>(lhs.callback)->getRecord()->version()
          ==
    * std::static_pointer_cast<GetCallback>(rhs.callback)->getRecord()->version();
}



void makeGetVersionOps(
  std::vector<KineticAsyncOperation>& ops,
  const std::shared_ptr<const std::string>& key
)
{
  auto sync = std::make_shared<KineticOperationSync>();
  for(auto o = ops.begin(); o != ops.end(); o++){
    auto cb   = std::make_shared<GetVersionCallback>(sync);
    o->callback = cb;
    o->function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
              const std::shared_ptr<const std::string>,
              const shared_ptr<GetVersionCallbackInterface>)>(
      &ThreadsafeNonblockingKineticConnection::GetVersion,
            std::placeholders::_1,
            std::cref(key),
            cb);
  }
}

KineticStatus KineticCluster::getVersion(
    const std::shared_ptr<const std::string>& key,
          std::shared_ptr<const std::string>& version
)
{ 
  auto ops = initialize(key, nData+nParity);
  makeGetVersionOps(ops, key);
  auto status = execute(ops);
  if(!status.ok()) return status;

  int count;
  auto& op = mostFrequent(ops, count, getVersionEqual);
  if(count<nData){
    return KineticStatus(
              StatusCode::CLIENT_IO_ERROR,
              "Unreadable: "+std::to_string((long long int)count)+
              " equal versions does not reach read quorum of "+
              std::to_string((long long int)nData)
            );
  }
  version.reset(new string(std::move(
        std::static_pointer_cast<GetVersionCallback>(op.callback)->getVersion()
  )));
  return status;
}


void makeGetOps(
  std::vector<KineticAsyncOperation>& ops,
  const std::shared_ptr<const std::string>& key
)
{
  auto sync = std::make_shared<KineticOperationSync>();
  for(auto o = ops.begin(); o != ops.end(); o++){
    auto cb = std::make_shared<GetCallback>(sync);
    o->callback = cb;
    o->function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
              const std::shared_ptr<const std::string>,
              const shared_ptr<GetCallbackInterface>)>(
      &ThreadsafeNonblockingKineticConnection::Get,
            std::placeholders::_1,
            std::cref(key),
            cb);
  }
}
/* Build a stripe vector from the records returned by a get operation. Only
 * accept values with valid CRC. */
std::vector< shared_ptr<const string> > getOperationToStripe(
    std::vector< KineticAsyncOperation >& ops,
    int& count,
    const std::shared_ptr<const string>& target_version
)
{
  std::vector< shared_ptr<const string> > stripe;
  count = 0;

  for(auto o = ops.begin(); o != ops.end(); o++){
    auto& record = std::static_pointer_cast<GetCallback>(o->callback)->getRecord();
    stripe.push_back(make_shared<const string>());

    if( record &&
       *record->version() == *target_version &&
        record->value() &&
        record->value()->size()
    ){
      /* validate the checksum */
      auto checksum = crc32(0,
                        (const Bytef*) record->value()->c_str(), 
                        record->value()->length()
                      );
      auto tag = std::to_string((long long unsigned int) checksum);
      if(tag == *record->tag()){
        stripe.back() = record->value();
        count++;
      }
    }
  }
  return std::move(stripe);
}

KineticStatus KineticCluster::get(
    const shared_ptr<const string>& key,
    bool skip_value,
    shared_ptr<const string>& version,
    shared_ptr<const string>& value
)
{
  if(skip_value)
    return getVersion(key,version);

  auto ops = initialize(key, nData+nParity);
  makeGetOps(ops, key);
  auto status = execute(ops);
  if(!status.ok()) return status;

  /* At least nData get operations succeeded. Validate that a
   * read quorum of nData operations returned a conforming version. */
  int count=0;
  auto& op = mostFrequent(ops, count, getRecordVersionEqual);
  if(count<nData){
    return KineticStatus(
              StatusCode::CLIENT_IO_ERROR,
              "Unreadable: "+std::to_string((long long int)count)+
              " equal versions does not reach read quorum of "+
              std::to_string((long long int)nData)
            );
  }
  auto target_version = std::static_pointer_cast<GetCallback>(op.callback)->getRecord()->version();
  auto stripe = getOperationToStripe(ops, count, target_version);

  /* count 0 -> empty value for key. */
  if(count==0){
    value = make_shared<const string>();
    version = target_version;
    return status;
  }

  /* missing blocks -> erasure code */
  if(count < stripe.size()){
    try{
        erasure->compute(stripe);
    }catch(const std::exception& e){
      return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, e.what());
    }
  }

  /* Create a single value from stripe values. */
  auto v = make_shared<string>();
  for(int i=0; i<nData; i++){
    v->append(stripe[i]->c_str());
  }
  /* Resize value to size encoded in version (to support unaligned value sizes). */
  v->resize( utility::uuidDecodeSize(target_version));
  value = std::move(v);
  version = target_version;
  return status;
}


void makePutOps(
  std::vector<KineticAsyncOperation>& ops,
  std::vector< shared_ptr<const string> >& stripe,
  const std::shared_ptr<const std::string>& key,
  const shared_ptr<const string>& version_new,
  const std::shared_ptr<const string>& version_old,
  WriteMode wmode
)
{
  auto sync = std::make_shared<KineticOperationSync>();

  for(int i=0; i<ops.size(); i++){
    auto& v = stripe[i];

    /* Generate Checksum. */
    auto checksum = crc32(0, (const Bytef*) v->c_str(), v->length());
    auto tag = std::make_shared<string>(
        std::to_string((long long unsigned int) checksum)
    );

    /* Construct record structure. */
    shared_ptr<KineticRecord> record(
      new KineticRecord(v, version_new, tag,
        com::seagate::kinetic::client::proto::Command_Algorithm_CRC32)
    );

    auto cb = std::make_shared<PutCallback>(sync);
    ops[i].callback = cb;
    ops[i].function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
            const shared_ptr<const string>,
            const shared_ptr<const string>,
            WriteMode,
            const shared_ptr<const KineticRecord>,
            const shared_ptr<PutCallbackInterface>,
            PersistMode)>(
    &ThreadsafeNonblockingKineticConnection::Put,
          std::placeholders::_1,
          std::cref(key),
          std::cref(version_old),
          wmode,
          record,
          cb,
          PersistMode::WRITE_BACK);
  }
}

KineticStatus KineticCluster::put(
    const shared_ptr<const string>& key,
    const shared_ptr<const string>& version_in,
    const shared_ptr<const string>& value,
    bool force,
    shared_ptr<const string>& version_out)
{
  auto ops = initialize(key, nData+nParity);

  /* Do not use version_in, version_out variables directly in case the client
     supplies the same pointer for both. */
  auto version_old = version_in ? version_in : make_shared<const string>();
  auto version_new = utility::uuidGenerateEncodeSize(value->size());

  /* Create a stripe vector by chunking up the value into nData data chunks
     and computing nParity parity chunks. */
  int chunk_size = (value->size() + nData-1) / (nData);
  std::vector< shared_ptr<const string> > stripe;
  for(int i=0; i<nData+nParity; i++){
    if(value->size() > i*chunk_size)
      stripe.push_back(
            make_shared<string>(value->substr(i*chunk_size, chunk_size))
            );
    else
      stripe.push_back(make_shared<string>());
  }
  try{
    /*Do not try to erasure code data if we are putting an empty key. The
      erasure coding would assume all chunks are missing. and throw an error.*/
    if(chunk_size)
      erasure->compute(stripe);
  }catch(const std::exception& e){
    return KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, e.what());
  }

  makePutOps(ops, stripe, key, version_new, version_old,
      force ? WriteMode::IGNORE_VERSION : WriteMode::REQUIRE_SAME_VERSION
  );
  auto status = execute(ops);

  if(status.ok()){
    version_out = version_new;
  }
  return status;
}


void makeDeleteOps(
    std::vector<KineticAsyncOperation>& ops,
    const shared_ptr<const string>& key,
    const shared_ptr<const string>& version,
    WriteMode wmode
)
{
  auto sync = std::make_shared<KineticOperationSync>();
  for(auto o = ops.begin(); o != ops.end(); o++){
    auto cb = std::make_shared<DeleteCallback>(sync);
    o->callback = cb;
    o->function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
              const shared_ptr<const string>,
              const shared_ptr<const string>,
              WriteMode,
              const shared_ptr<SimpleCallbackInterface>,
              PersistMode)>(
      &ThreadsafeNonblockingKineticConnection::Delete,
            std::placeholders::_1,
            std::cref(key),
            std::cref(version),
            wmode,
            cb,
            PersistMode::WRITE_BACK);
  }
}

KineticStatus KineticCluster::remove(
    const shared_ptr<const string>& key,
    const shared_ptr<const string>& version,
    bool force)
{
  auto ops = initialize(key, nData+nParity);
  makeDeleteOps(ops,key, version,
      force ? WriteMode::IGNORE_VERSION : WriteMode::REQUIRE_SAME_VERSION
  );
  return execute(ops);
}

void makeRangeOps(
    std::vector<KineticAsyncOperation>& ops,
    const std::shared_ptr<const std::string>& start_key,
    const std::shared_ptr<const std::string>& end_key,
    int maxRequested
)
{
  auto sync = std::make_shared<KineticOperationSync>();
  for(auto o = ops.begin(); o != ops.end(); o++){
    auto cb = std::make_shared<RangeCallback>(sync);
    o->callback = cb;
    o->function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
            const shared_ptr<const string>, bool,
            const shared_ptr<const string>, bool,
            bool,
            int32_t,
            const shared_ptr<GetKeyRangeCallbackInterface>)>(
    &ThreadsafeNonblockingKineticConnection::GetKeyRange,
          std::placeholders::_1,
          std::cref(start_key), true,
          std::cref(end_key), true,
          false,
          maxRequested,
          cb);
  }
}

KineticStatus KineticCluster::range(
    const std::shared_ptr<const std::string>& start_key,
    const std::shared_ptr<const std::string>& end_key,
    int maxRequested,
    std::unique_ptr< std::vector<std::string> >& keys)
{
  auto ops = initialize(start_key, nData+nParity);
  makeRangeOps(ops, start_key, end_key, maxRequested);
  auto status = execute(ops);
  if(!status.ok()) return status;

  /* Process Results stored in Callbacks. */

  /* merge in set to eliminate doubles  */
  std::set<std::string> set;
  for(auto o = ops.cbegin(); o != ops.cend(); o++){
    set.insert(
      std::static_pointer_cast<RangeCallback>(o->callback)->getKeys()->begin(),
      std::static_pointer_cast<RangeCallback>(o->callback)->getKeys()->end()
    );
  }

  /* assign to output parameter and cut excess results */
  keys.reset( new std::vector<std::string>(set.begin(),set.end()) );
  if(keys->size() > maxRequested)
    keys->resize(maxRequested);

  return status;
}

void makeGetLogOps(
    std::vector<KineticAsyncOperation>& ops,
    const std::vector<Command_GetLog_Type>& types
)
{
  auto sync = std::make_shared<KineticOperationSync>();
  for(auto o = ops.begin(); o != ops.end(); o++){
    auto cb = std::make_shared<GetLogCallback>(sync);
    o->callback = cb;
    o->function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
              const vector<Command_GetLog_Type>&,
              const shared_ptr<GetLogCallbackInterface>)>(
      &ThreadsafeNonblockingKineticConnection::GetLog,
            std::placeholders::_1,
            std::cref(types),
            cb);
  }
}

KineticStatus KineticCluster::getLog(std::vector<Command_GetLog_Type> types)
{
  /* This function is executed in a background thread. Make sure it never
     ever throws anything, as otherwise the whole process terminates. */
  try{

  auto ops = initialize( make_shared<string>("all"), connections.size() );
  makeGetLogOps(ops, types);
  auto status = execute(ops);

  /* Step 4) Evaluate Operation Result. */
  std::lock_guard<std::mutex> lck(getlog_mutex);
  getlog_status = status;
  getlog_outstanding = false;
  if(!getlog_status.ok())
    return getlog_status;

  /* Step 5) Process Results stored in Callbacks. */
  clustersize={0,0};
  for(auto o = ops.begin(); o != ops.end(); o++){
    if(!o->callback->getResult().ok())
      continue;

    if(std::find(types.begin(), types.end(),
        Command_GetLog_Type::Command_GetLog_Type_CAPACITIES) != types.end()){
      const auto& c = std::static_pointer_cast<GetLogCallback>
                          (o->callback)->getLog()->capacity;
      clustersize.bytes_total += c.nominal_capacity_in_bytes;
      clustersize.bytes_free  += c.nominal_capacity_in_bytes -
                          (c.nominal_capacity_in_bytes * c.portion_full);
    }
    if(std::find(types.begin(), types.end(),
        Command_GetLog_Type::Command_GetLog_Type_LIMITS) != types.end()){
      const auto& l = std::static_pointer_cast<GetLogCallback>
                          (o->callback)->getLog()->limits;
      clusterlimits.max_key_size = l.max_key_size;
      clusterlimits.max_value_size = l.max_value_size;
      clusterlimits.max_version_size = l.max_version_size;
    }
  }
  return getlog_status;

  }catch(...){}
}

const ClusterLimits& KineticCluster::limits() const
{
  return clusterlimits;
}

KineticStatus KineticCluster::size(ClusterSize& size)
{
  std::lock_guard<std::mutex> lck(getlog_mutex);
  if(getlog_outstanding == false){
    getlog_outstanding = true;
    std::vector<Command_GetLog_Type> v =
        {Command_GetLog_Type::Command_GetLog_Type_CAPACITIES};
    std::thread(&KineticCluster::getLog, this, std::move(v)).detach();
  }

  if(getlog_status.ok())
    size = clustersize;

  return getlog_status;
}


std::vector<KineticAsyncOperation> KineticCluster::initialize(
    const shared_ptr<const string>& key,
    size_t size)
{
  auto index = std::hash<std::string>()(*key);
  std::vector<KineticAsyncOperation> ops;

  while(size){
    index = (index+1) % connections.size();
    ops.push_back(
      KineticAsyncOperation{
          0,
          std::shared_ptr<kio::KineticCallback>(),
          &connections[index]
      }
    );
    size--;
  }
  return ops;
}




KineticStatus KineticCluster::execute(
    std::vector< KineticAsyncOperation >& ops)
{
  using std::chrono::duration_cast;
  using std::chrono::system_clock;
  using std::chrono::seconds;

  auto& sync = ops.begin()->callback->getSync();
  std::vector<kinetic::HandlerKey> hkeys;

   /* Call functions on connections */
  for(auto o = ops.begin(); o != ops.end(); o++){
    try{
      auto con  = o->connection->get();
      hkeys.push_back( o->function( con ) );
//      printf("Called function on connection %p\n",o->connection);
      
      fd_set a; int fd;
      if(!con->Run(&a,&a,&fd))
        throw std::runtime_error("Connection unusable.");
    }
    catch(const std::exception& e) {
      o->callback->OnResult(
        KineticStatus(StatusCode::REMOTE_REMOTE_CONNECTION_ERROR, e.what())
      );
    }
  }

  /* Wait until sufficient requests returned or we pass operation timeout. */
  {
    system_clock::time_point timeout_time = system_clock::now() + operation_timeout;
    std::unique_lock<std::mutex> lck(sync->mutex);
    while (sync->outstanding && system_clock::now() < timeout_time){
      sync->cv.wait_until(lck, timeout_time);
    }
  }

  /* timeout any unfinished request, do not mark the associated connection
   * as broken, if Run() failed it has already been done, otherwise it is an
   * honest time out. */
  for(int i=0; i<ops.size(); i++){
    if(!ops[i].callback->finished()){
        ops[i].connection->get()->RemoveHandler(hkeys[i]);
        ops[i].callback->OnResult(KineticStatus(StatusCode::CLIENT_IO_ERROR, "Network timeout"));
      }
  }

  /* In almost all cases this loop will terminate after a single iteration. */
  int count=0;
  auto& op = mostFrequent(ops, count, resultsEqual);
  if(count<nData){
    return KineticStatus(
              StatusCode::CLIENT_IO_ERROR,
              "Failed to get quorum of conforming return results from drives."
            );
  }
  return op.callback->getResult();
}

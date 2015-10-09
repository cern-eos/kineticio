#include <stdlib.h>
#include <fstream>
#include <sstream>
#include "Logging.hh"
#include "ClusterMap.hh"

using namespace kio;


/* Read file located at path into string buffer and return it. */
static std::string readfile(const char *path)
{
  std::ifstream file(path);
  /* Unlimted buffer size so reading in large cluster files works. */
  file.rdbuf()->pubsetbuf(0, 0);
  std::stringstream buffer;
  buffer << file.rdbuf();
  if(buffer.str().empty())
    throw kio_exception(EIO,"File ",path," could not be read in.");
  return buffer.str();
}

/* Printing errors initializing static global object to stderr.*/
ClusterMap::ClusterMap()
{
  try{
    loadConfiguration();
  }catch(const std::exception& e){
    kio_warning(e.what());
  }
}

void ClusterMap::loadConfiguration()
{
  /* get file names */
  const char *location = getenv("KINETIC_DRIVE_LOCATION");
  if (!location)
    throw kio_exception(EINVAL,"KINETIC_DRIVE_LOCATION not set.");

  const char *security = getenv("KINETIC_DRIVE_SECURITY");
  if (!security)
    throw kio_exception(EINVAL,"KINETIC_DRIVE_SECURITY not set.");

  const char *cluster = getenv("KINETIC_CLUSTER_DEFINITION");
  if (!cluster)
    throw kio_exception(EINVAL,"KINETIC_CLUSTER_DEFINITION not set.");

  /* get file contents */
  std::string location_data = readfile(location);
  std::string security_data = readfile(security);
  std::string cluster_data = readfile(cluster);

  std::lock_guard<std::mutex> lock(mutex);
  clustermap.clear();
  drivemap.clear();

  /* parse files */
  parseJson(location_data, filetype::location);
  parseJson(security_data, filetype::security);
  parseJson(cluster_data, filetype::cluster);

  if(!ecCache)
    ecCache.reset(new LRUCache<std::string, std::shared_ptr<ErasureCoding>>(
        configuration.num_erasure_codings
    ));
  else
    ecCache->setCapacity(configuration.num_erasure_codings);

  if(!dataCache)
    dataCache.reset(new ClusterChunkCache(
        configuration.stripecache_target, configuration.stripecache_capacity,
        configuration.background_io_threads, configuration.background_io_queue_capacity,
        configuration.readahead_window_size
    ));
  else
    dataCache->changeConfiguration(configuration.stripecache_target, configuration.stripecache_capacity,
                                   configuration.background_io_threads, configuration.background_io_queue_capacity,
                                   configuration.readahead_window_size
    );

  if(!listener)
    listener.reset(new SocketListener());
}

ClusterChunkCache& ClusterMap::getCache()
{
  return *dataCache;
}

void ClusterMap::fillArgs(const KineticClusterInfo &ki,
                          std::shared_ptr<ErasureCoding>& ec,
                          std::vector<std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>>& cops)
{
  for (auto wwn = ki.drives.begin(); wwn != ki.drives.end(); wwn++) {
    if (!drivemap.count(*wwn))
      throw kio_exception(ENODEV, "Nonexisting drive wwn requested: ", *wwn);
    cops.push_back(drivemap.at(*wwn));
  }

  auto ectype = utility::Convert::toString(ki.numData, "-", ki.numParity);
  try{
    ec = ecCache->get(ectype);
  }
  catch(const std::out_of_range& e){
    ec = std::make_shared<ErasureCoding>(ki.numData, ki.numParity, configuration.num_erasure_coding_tables);
    ecCache->add(ectype, ec);
  }
}

std::unique_ptr<KineticAdminCluster> ClusterMap::getAdminCluster(
      const std::string& id,
      AdminClusterInterface::OperationTarget target, 
      size_t numthreads)
{
  if(!listener)
    throw kio_exception(EACCES, "ClusterMap not properly initialized. Check your json files.");

  std::unique_lock<std::mutex> locker(mutex);
  if (!clustermap.count(id))
    throw kio_exception(ENODEV, "Nonexisting cluster id requested: ", id);

  std::vector<std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>> cops;
  std::shared_ptr<ErasureCoding> ec;
  KineticClusterInfo &ki = clustermap.at(id);
  fillArgs(ki, ec, cops);

  return std::unique_ptr<KineticAdminCluster>(new KineticAdminCluster(target, numthreads,
      ki.numData, ki.numParity, ki.blockSize,
      cops, ki.min_reconnect_interval, ki.operation_timeout,
      ec, *listener)
  );
}

std::shared_ptr<ClusterInterface> ClusterMap::getCluster(const std::string &id)
{
  if(!listener)
    throw kio_exception(EACCES, "ClusterMap not properly initialized. Check your json files.");

  std::unique_lock<std::mutex> locker(mutex);
  if (!clustermap.count(id))
    throw kio_exception(ENODEV, "Nonexisting cluster id requested: ", id);

  KineticClusterInfo &ki = clustermap.at(id);
  if (!ki.cluster) {
    std::vector<std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>> cops;
    std::shared_ptr<ErasureCoding> ec;
    fillArgs(ki, ec, cops);

    ki.cluster = std::make_shared<KineticCluster>(
        ki.numData, ki.numParity, ki.blockSize,
        cops, ki.min_reconnect_interval, ki.operation_timeout,
        ec, *listener
    );
  }
  return ki.cluster;
}

static std::string loadJsonStringEntry(struct json_object* obj, const char* key)
{
  struct json_object *tmp = NULL;
  if(!json_object_object_get_ex(obj, key, &tmp))
    throw kio_exception(EINVAL, "Failed reading in key ",key);
  return json_object_get_string(tmp);
}

static int loadJsonIntEntry(struct json_object* obj, const char* key)
{
  struct json_object *tmp = NULL;
  if(!json_object_object_get_ex(obj, key, &tmp))
    throw kio_exception(EINVAL, "Failed reading in key ",key);
  return json_object_get_int(tmp);
}

void ClusterMap::parseDriveLocation(struct json_object* drive)
{
  struct json_object *tmp = NULL;
  struct json_object *host = NULL;
  std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions> kops;

  std::string id = loadJsonStringEntry(drive, "wwn");

  if (!json_object_object_get_ex(drive, "inet4", &tmp))
    throw kio_exception(EINVAL, "Drive with wwn ", id, " is missing location information");

  host = json_object_array_get_idx(tmp, 0);
  if (!host)
    throw kio_exception(EINVAL, "Drive with wwn ", id, " is missing location information");
  kops.first.host = json_object_get_string(host);

  host = json_object_array_get_idx(tmp, 1);
  if (host)
    kops.second.host = json_object_get_string(host);
  else
    kops.second.host = kops.first.host;

  kops.first.port = kops.second.port = loadJsonIntEntry(drive, "port");
  kops.first.use_ssl = kops.second.use_ssl = false;
  drivemap.insert(std::make_pair(id, kops));
}

void ClusterMap::parseDriveSecurity(struct json_object* drive)
{
  std::string id = loadJsonStringEntry(drive, "wwn");
  /* Require that drive info has been scanned already.*/
  if (!drivemap.count(id))
    throw kio_exception(EINVAL, "Security for unknown drive with wwn ", id, " provided.");

  auto &kops = drivemap.at(id);
  kops.first.user_id = kops.second.user_id = loadJsonIntEntry(drive, "userId");
  kops.first.hmac_key = kops.second.hmac_key =  loadJsonStringEntry(drive, "key");
}

void ClusterMap::parseClusterInformation(struct json_object* cluster)
{
  struct KineticClusterInfo cinfo;

  std::string id = loadJsonStringEntry(cluster, "clusterID");
  cinfo.numData = loadJsonIntEntry(cluster, "numData");
  cinfo.numParity = loadJsonIntEntry(cluster, "numParity");

  cinfo.blockSize = loadJsonIntEntry(cluster, "chunkSizeKB");
  cinfo.blockSize *= 1024;

  cinfo.min_reconnect_interval = std::chrono::seconds(loadJsonIntEntry(cluster, "minReconnectInterval"));
  cinfo.operation_timeout = std::chrono::seconds(loadJsonIntEntry(cluster, "timeout"));

  struct json_object *list = NULL;
  if (!json_object_object_get_ex(cluster, "drives", &list))
    throw kio_exception(EINVAL, "Could not find drive list for cluster ",id);

  json_object *drive = NULL;
  int num_drives = json_object_array_length(list);

  for (int i = 0; i < num_drives; i++) {
    drive = json_object_array_get_idx(list, i);
    cinfo.drives.push_back(loadJsonStringEntry(drive, "wwn"));
  }
  clustermap.insert(std::make_pair(id, cinfo));
}



void ClusterMap::parseConfiguration(struct json_object* config)
{
  configuration.stripecache_target = loadJsonIntEntry(config, "cacheTargetSizeMB");
  configuration.stripecache_target *= 1024*1024;

  configuration.stripecache_capacity = loadJsonIntEntry(config, "cacheCapacityMB");
  configuration.stripecache_capacity *= 1024*1024;

  configuration.readahead_window_size = loadJsonIntEntry(config, "maxReadaheadWindow");
  configuration.background_io_threads = loadJsonIntEntry(config, "maxBackgroundIoThreads");
  configuration.background_io_queue_capacity = loadJsonIntEntry(config, "maxBackgroundIoQueue");
  configuration.num_erasure_codings = loadJsonIntEntry(config, "erasureCodings");
  configuration.num_erasure_coding_tables = loadJsonIntEntry(config, "erasureDecodingTables");
}


void ClusterMap::parseJson(const std::string& filedata, filetype type)
{
  struct json_object *root = json_tokener_parse(filedata.c_str());
  if (!root)
    throw std::runtime_error("Failed initializing json token parser.");

  try{
    struct json_object *element = NULL;
    struct json_object *list = NULL;
    std::string typestring;
    switch (type) {
      case filetype::location:
        typestring = "location";
        break;
      case filetype::security:
        typestring = "security";
        break;
      case filetype::cluster:
        typestring = "cluster";
        if (!json_object_object_get_ex(root, "configuration", &list))
          throw kio_exception(EINVAL, "No configuration entry found");
        parseConfiguration(list);
        break;
    }

    if (!json_object_object_get_ex(root, typestring.c_str(), &list))
      throw kio_exception(EINVAL, "No ",typestring.c_str(), " entry found");

    int num_drives = json_object_array_length(list);
    for (int i = 0; i < num_drives; i++) {
      element = json_object_array_get_idx(list, i);
      switch (type) {
        case filetype::location:
          parseDriveLocation(element);
          break;
        case filetype::security:
          parseDriveSecurity(element);
          break;
        case filetype::cluster:
          parseClusterInformation(element);
          break;
      }
    }
  }catch(const std::exception& e){
    json_object_put(root);
    throw e;
  }
  json_object_put(root);
}
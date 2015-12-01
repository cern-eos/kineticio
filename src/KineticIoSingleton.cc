#include "KineticIoSingleton.hh"
#include "Logging.hh"
#include <stdlib.h>
#include <fstream>
#include <sstream>
#include <iostream>

using namespace kio;

KineticIoSingleton::KineticIoSingleton()
{
  try{
    loadConfiguration();
  }catch(const std::exception& e){
    printf("Error loading configuration: %s\n",e.what());
    kio_warning(e.what());
  }
}

KineticIoSingleton& KineticIoSingleton::getInstance()
{
   static KineticIoSingleton sb;
   return sb;
}

DataCache& KineticIoSingleton::cache()
{
  if(!dataCache)
    throw kio_exception(EINVAL,"DataCache not initialized.");
  return *dataCache;
}

ClusterMap& KineticIoSingleton::cmap()
{
  if(!clusterMap)
    throw kio_exception(EINVAL,"Cluster Map not initialized.");
  return *clusterMap;
}

BackgroundOperationHandler& KineticIoSingleton::threadpool()
{
  throw std::logic_error("not implemented");  
}

/* Utility functions for this class only. */
namespace{
  /* Read file located at path into string buffer and return it. */
  std::string readfile(const char *path)
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
  
  std::string loadJsonStringEntry(struct json_object* obj, const char* key)
  {
    struct json_object *tmp = NULL;
    if(!json_object_object_get_ex(obj, key, &tmp))
      throw kio_exception(EINVAL, "Failed reading in key ",key);
    return json_object_get_string(tmp);
  }

  int loadJsonIntEntry(struct json_object* obj, const char* key)
  {
    struct json_object *tmp = NULL;
    if(!json_object_object_get_ex(obj, key, &tmp))
      throw kio_exception(EINVAL, "Failed reading in key ",key);
    return json_object_get_int(tmp);
  }
  
  void put_json(json_object* json_root)
  {
    json_object_put(json_root);
  }
}

void KineticIoSingleton::loadConfiguration()
{
  const char *location = getenv("KINETIC_DRIVE_LOCATION");
  if (!location)
    throw kio_exception(EINVAL,"KINETIC_DRIVE_LOCATION not set.");

  const char *security = getenv("KINETIC_DRIVE_SECURITY");
  if (!security)
    throw kio_exception(EINVAL,"KINETIC_DRIVE_SECURITY not set.");

  const char *cluster = getenv("KINETIC_CLUSTER_DEFINITION");
  if (!cluster)
    throw kio_exception(EINVAL,"KINETIC_CLUSTER_DEFINITION not set.");

  /* If the environment variables contain the json content, use them directly, otherwise get file contents */
  std::string location_data = location[0] == '/' || location[0] == '.' ? readfile(location) : location ;
  std::string security_data = security[0] == '/' || security[0] == '.' ? readfile(security) : security ;
  std::string cluster_data = cluster[0] == '/' || cluster[0] == '.' ? readfile(cluster) : cluster ;
  
  typedef std::unique_ptr<json_object, void(*)(json_object*)> unique_json_ptr;
  unique_json_ptr location_root(json_tokener_parse(location_data.c_str()), &put_json);
  unique_json_ptr security_root(json_tokener_parse(security_data.c_str()), &put_json);
  unique_json_ptr cluster_root(json_tokener_parse(cluster_data.c_str()), &put_json);
  
  if (!location_root || !security_root || !cluster_root)
    throw std::runtime_error("Failed initializing json token parser.");

  struct json_object* o1 = NULL;
  struct json_object* o2 = NULL;

  if (!json_object_object_get_ex(cluster_root.get(), "configuration", &o1))
      throw kio_exception(EINVAL, "No configuration entry found");
  auto config = parseConfiguration(o1);

  if (!json_object_object_get_ex(location_root.get(), "location", &o1))
      throw kio_exception(EINVAL, "No location entry found");
  if (!json_object_object_get_ex(security_root.get(), "security", &o2))
      throw kio_exception(EINVAL, "No security entry found");
  auto driveInfo = parseDrives(o1, o2);

  if (!json_object_object_get_ex(cluster_root.get(), "cluster", &o1))
      throw kio_exception(EINVAL, "No cluster entry found");
  auto clusterInfo = parseClusters(o1);
  
  std::lock_guard<std::mutex> lock(mutex);
  configuration = config;
  
  if(!clusterMap)
    clusterMap.reset(new ClusterMap(clusterInfo, driveInfo));
  else
    clusterMap->reset(clusterInfo, driveInfo);
  
  if(!dataCache)
    dataCache.reset(new DataCache(
        configuration.stripecache_capacity,
        configuration.background_io_threads, configuration.background_io_queue_capacity,
        configuration.readahead_window_size
    ));
  else
    dataCache->changeConfiguration(configuration.stripecache_capacity,
                                   configuration.background_io_threads, configuration.background_io_queue_capacity,
                                   configuration.readahead_window_size
    );
}

std::unordered_map<std::string, std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>> KineticIoSingleton::parseDrives(struct json_object* locations, struct json_object* security)
{
  std::unordered_map<std::string, std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>> driveInfo;
  
  std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions> kops;
  struct json_object *host = NULL;
  struct json_object *tmp = NULL;
    
  int length = json_object_array_length(locations);
  for (int i = 0; i < length; i++) {
    auto drive = json_object_array_get_idx(locations, i);
    auto id = loadJsonStringEntry(drive, "wwn");

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
    driveInfo.insert(std::make_pair(id, kops));
  }
  
  length = json_object_array_length(security);
  for (int i = 0; i < length; i++) {
    auto drive = json_object_array_get_idx(security, i);
    auto id = loadJsonStringEntry(drive, "wwn");
     /* Require that drive info has been scanned already.*/
    if (!driveInfo.count(id))
      throw kio_exception(EINVAL, "Security for unknown drive with wwn ", id, " provided.");

    auto &kops = driveInfo.at(id);
    kops.first.user_id = kops.second.user_id = loadJsonIntEntry(drive, "userId");
    kops.first.hmac_key = kops.second.hmac_key =  loadJsonStringEntry(drive, "key");
  }
  
  return driveInfo;
}

std::unordered_map<std::string, ClusterInformation> KineticIoSingleton::parseClusters(struct json_object* clusters)
{
  std::unordered_map<std::string, ClusterInformation> clusterInfo;
   
  int num_clusters = json_object_array_length(clusters);   
  for (int i = 0; i < num_clusters; i++) {
    auto cluster = json_object_array_get_idx(clusters, i); 

    std::string id = loadJsonStringEntry(cluster, "clusterID");
    
    struct ClusterInformation cinfo;
     
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
    clusterInfo.insert(std::make_pair(id, cinfo));
  }
  return clusterInfo;
}

KineticIoSingleton::Configuration KineticIoSingleton::parseConfiguration(struct json_object* config)
{
  KineticIoSingleton::Configuration c; 
  c.stripecache_capacity = loadJsonIntEntry(config, "cacheCapacityMB");
  c.stripecache_capacity *= 1024*1024;

  c.readahead_window_size = loadJsonIntEntry(config, "maxReadaheadWindow");
  c.background_io_threads = loadJsonIntEntry(config, "maxBackgroundIoThreads");
  c.background_io_queue_capacity = loadJsonIntEntry(config, "maxBackgroundIoQueue");
  
  return c;
}

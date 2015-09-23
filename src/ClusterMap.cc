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
  return buffer.str();
}

/* Printing errors initializing static global object to stderr.*/
ClusterMap::ClusterMap()
{
  /* get file names */
  const char *location = getenv("KINETIC_DRIVE_LOCATION");
  if (!location) {
    fprintf(stderr, "KINETIC_DRIVE_LOCATION not set.\n");
    return;
  }
  const char *security = getenv("KINETIC_DRIVE_SECURITY");
  if (!security) {
    fprintf(stderr, "KINETIC_DRIVE_SECURITY not set.\n");
    return;
  }
  const char *cluster = getenv("KINETIC_CLUSTER_DEFINITION");
  if (!cluster) {
    fprintf(stderr, "KINETIC_CLUSTER_DEFINITION not set.\n");
    return;
  }
  /* get file contents */
  std::string location_data = readfile(location);
  if (location_data.empty()) {
    fprintf(stderr, "LIBKINETICIO: File '%s' could not be read in.\n", location);
    return;
  }
  std::string security_data = readfile(security);
  if (security_data.empty()) {
    fprintf(stderr, "LIBKINETICIO: File '%s' could not be read in.\n", security);
    return;
  }
  std::string cluster_data = readfile(cluster);
  if (cluster_data.empty()) {
    fprintf(stderr, "LIBKINETICIO: File '%s' could not be read in.\n", cluster);
    return;
  }

  /* parse files */
  if (parseJson(location_data, filetype::location)) {
    fprintf(stderr, "LIBKINETICIO: Error while parsing location json file '%s\n", location);
    return;
  }
  if (parseJson(security_data, filetype::security)) {
    fprintf(stderr, "LIBKINETICIO: Error while parsing security json file '%s\n", security);
    return;
  }
  if (parseJson(cluster_data, filetype::cluster)) {
    fprintf(stderr, "LIBKINETICIO: Error while parsing cluster json file '%s\n", cluster);
    return;
  }

  ecCache.reset(new LRUCache<std::string, std::shared_ptr<ErasureCoding>>(
      configuration.num_erasure_codings
  ));

  dataCache.reset(new ClusterChunkCache(
      configuration.stripecache_target, configuration.stripecache_capacity,
      configuration.background_io_threads, configuration.background_io_queue_capacity
  ));

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

std::unique_ptr<KineticAdminCluster> ClusterMap::getAdminCluster(const std::string& id)
{
  if(!listener)
    throw kio_exception(EACCES, "ClusterMap not properly initialized. Check your json files.");
  if (!clustermap.count(id))
    throw kio_exception(ENODEV, "Nonexisting cluster id requested: ", id);

  std::vector<std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>> cops;
  std::shared_ptr<ErasureCoding> ec;
  KineticClusterInfo &ki = clustermap.at(id);
  fillArgs(ki, ec, cops);

  return std::unique_ptr<KineticAdminCluster>(new KineticAdminCluster(
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

int ClusterMap::parseDriveLocation(struct json_object *drive)
{
  struct json_object *tmp = NULL;
  struct json_object *host = NULL;
  std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions> kops;

  if (!json_object_object_get_ex(drive, "wwn", &tmp))
    return -EINVAL;
  std::string id = json_object_get_string(tmp);

  if (!json_object_object_get_ex(drive, "inet4", &tmp))
    return -EINVAL;

  host = json_object_array_get_idx(tmp, 0);
  if (!host)
    return -EINVAL;
  kops.first.host = json_object_get_string(host);

  host = json_object_array_get_idx(tmp, 1);
  if (!host)
    kops.second.host = kops.first.host;
  else
    kops.second.host = json_object_get_string(host);

  if (!json_object_object_get_ex(drive, "port", &tmp))
    return -EINVAL;
  kops.first.port = kops.second.port = json_object_get_int(tmp);

  kops.first.use_ssl = kops.second.use_ssl = false;
  drivemap.insert(std::make_pair(id, kops));
  return 0;
}

int ClusterMap::parseDriveSecurity(struct json_object *drive)
{
  struct json_object *tmp = NULL;
  if (!json_object_object_get_ex(drive, "wwn", &tmp))
    return -EINVAL;
  std::string id = json_object_get_string(tmp);

  /* Require that drive info has been scanned already.*/
  if (!drivemap.count(id))
    return -ENODEV;

  auto &kops = drivemap.at(id);

  if (!json_object_object_get_ex(drive, "userId", &tmp))
    return -EINVAL;
  kops.first.user_id = kops.second.user_id = json_object_get_int(tmp);

  if (!json_object_object_get_ex(drive, "key", &tmp))
    return -EINVAL;
  kops.first.hmac_key = kops.second.hmac_key = json_object_get_string(tmp);
  return 0;
}

int ClusterMap::parseClusterInformation(struct json_object *cluster)
{
  struct json_object *tmp = NULL;
  if (!json_object_object_get_ex(cluster, "clusterID", &tmp))
    return -EINVAL;
  std::string id = json_object_get_string(tmp);

  struct KineticClusterInfo cinfo;

  if (!json_object_object_get_ex(cluster, "numData", &tmp))
    return -EINVAL;
  cinfo.numData = json_object_get_int(tmp);

  if (!json_object_object_get_ex(cluster, "numParity", &tmp))
    return -EINVAL;
  cinfo.numParity = json_object_get_int(tmp);

  if (!json_object_object_get_ex(cluster, "chunkSizeKB", &tmp))
    return -EINVAL;
  cinfo.blockSize = json_object_get_int(tmp);
  cinfo.blockSize *= 1024;

  if (!json_object_object_get_ex(cluster, "minReconnectInterval", &tmp))
    return -EINVAL;
  cinfo.min_reconnect_interval = std::chrono::seconds(json_object_get_int(tmp));

  if (!json_object_object_get_ex(cluster, "timeout", &tmp))
    return -EINVAL;
  cinfo.operation_timeout = std::chrono::seconds(json_object_get_int(tmp));

  struct json_object *list = NULL;
  if (!json_object_object_get_ex(cluster, "drives", &list))
    return -EINVAL;

  json_object *drive = NULL;
  int num_drives = json_object_array_length(list);

  for (int i = 0; i < num_drives; i++) {
    drive = json_object_array_get_idx(list, i);
    if (!json_object_object_get_ex(drive, "wwn", &tmp))
      return -EINVAL;
    cinfo.drives.push_back(json_object_get_string(tmp));
  }
  clustermap.insert(std::make_pair(id, cinfo));
  return 0;
}

int ClusterMap::parseConfiguration(struct json_object* config)
{
  struct json_object *tmp = NULL;
  if (!json_object_object_get_ex(config, "cacheTargetSizeMB", &tmp))
    return -EINVAL;
  configuration.stripecache_target = json_object_get_int(tmp);
  configuration.stripecache_target *= 1024*1024;

  if (!json_object_object_get_ex(config, "cacheCapacityMB", &tmp))
    return -EINVAL;
  configuration.stripecache_capacity = json_object_get_int(tmp);
  configuration.stripecache_capacity *= 1024*1024;

  if (!json_object_object_get_ex(config, "maxBackgroundIoThreads", &tmp))
    return -EINVAL;
  configuration.background_io_threads = json_object_get_int(tmp);

  if (!json_object_object_get_ex(config, "maxBackgroundIoQueue", &tmp))
    return -EINVAL;
  configuration.background_io_queue_capacity = json_object_get_int(tmp);

  if (!json_object_object_get_ex(config, "erasureCodings", &tmp))
    return -EINVAL;
  configuration.num_erasure_codings = json_object_get_int(tmp);

  if (!json_object_object_get_ex(config, "erasureDecodingTables", &tmp))
    return -EINVAL;
  configuration.num_erasure_coding_tables = json_object_get_int(tmp);
  return 0;
}


int ClusterMap::parseJson(const std::string &filedata, filetype type)
{
  struct json_object *root = json_tokener_parse(filedata.c_str());
  if (!root)
    return -EINVAL;

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
      if (!json_object_object_get_ex(root, "configuration", &list) || parseConfiguration(list)) {
        json_object_put(root);
        return -EINVAL;
      }
      break;
  }

  if (!json_object_object_get_ex(root, typestring.c_str(), &list)) {
    json_object_put(root);
    return -EINVAL;
  }

  int num_drives = json_object_array_length(list);
  int err = 0;
  for (int i = 0; i < num_drives && !err; i++) {
    element = json_object_array_get_idx(list, i);
    switch (type) {
      case filetype::location:
        err = parseDriveLocation(element);
        break;
      case filetype::security:
        err = parseDriveSecurity(element);
        break;
      case filetype::cluster:
        err = parseClusterInformation(element);
        break;
    }
  }
  json_object_put(root);
  return err;
}
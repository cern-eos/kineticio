#include <stdlib.h>
#include <fstream>
#include <sstream>
#include "ClusterMap.hh"
#include "KineticCluster.hh"
#include "LoggingException.hh"

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
ClusterMap::ClusterMap() :
    ecCache(10)
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
    fprintf(stderr, "File '%s' could not be read in.\n", location);
    return;
  }
  std::string security_data = readfile(security);
  if (security_data.empty()) {
    fprintf(stderr, "File '%s' could not be read in.\n", security);
    return;
  }
  std::string cluster_data = readfile(cluster);
  if (cluster_data.empty()) {
    fprintf(stderr, "File '%s' could not be read in.\n", cluster);
    return;
  }

  /* parse files */
  if (parseJson(location_data, filetype::location)) {
    fprintf(stderr, "Error while parsing location json file '%s\n", location);
    return;
  }
  if (parseJson(security_data, filetype::security)) {
    fprintf(stderr, "Error while parsing security json file '%s\n", security);
    return;
  }
  if (parseJson(cluster_data, filetype::cluster)) {
    fprintf(stderr, "Error while parsing cluster json file '%s\n", cluster);
    return;
  }
}

ClusterMap::~ClusterMap()
{
}

std::shared_ptr<ClusterInterface>  ClusterMap::getCluster(const std::string &id)
{
  std::unique_lock<std::mutex> locker(mutex);

  if (!listener)
    listener.reset(new SocketListener());

  if (!clustermap.count(id))
    throw LoggingException(ENODEV, __FUNCTION__, __FILE__, __LINE__, "Nonexisting "
                                                                         "cluster id '" + id + "' requested.");

  KineticClusterInfo &ki = clustermap.at(id);
  if (!ki.cluster) {

    auto ectype = std::to_string((long long int) ki.numData) + "-" +
                  std::to_string((long long int) ki.numParity);
    if (!ecCache.exists(ectype)) {
      ecCache.put(ectype,
                  std::make_shared<ErasureCoding>(ki.numData, ki.numParity)
      );
    }

    std::vector<std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>> cops;
    for (auto wwn = ki.drives.begin(); wwn != ki.drives.end(); wwn++) {
      if (!drivemap.count(*wwn))
        throw LoggingException(ENODEV, __FUNCTION__, __FILE__, __LINE__, "Nonexisting "
                                                                             "drive wwn '" + *wwn + "' requested.");
      cops.push_back(drivemap.at(*wwn));
    }

    ki.cluster = std::make_shared<KineticCluster>(
        ki.numData, ki.numParity,
        cops, ki.min_reconnect_interval, ki.operation_timeout,
        ecCache.get(ectype),
        *listener
    );
  }
  return ki.cluster;
}

size_t ClusterMap::getSize()
{
  return clustermap.size();
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
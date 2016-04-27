/************************************************************************
 * KineticIo - a file io interface library to kinetic devices.          *
 *                                                                      *
 * This Source Code Form is subject to the terms of the Mozilla         *
 * Public License, v. 2.0. If a copy of the MPL was not                 *
 * distributed with this file, You can obtain one at                    *
 * https://mozilla.org/MP:/2.0/.                                        *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but is provided AS-IS, WITHOUT ANY WARRANTY; including without       *
 * the implied warranty of MERCHANTABILITY, NON-INFRINGEMENT or         *
 * FITNESS FOR A PARTICULAR PURPOSE. See the Mozilla Public             *
 * License for more details.                                            *
 ************************************************************************/

#include "KineticIoSingleton.hh"
#include "Logging.hh"
#include <fstream>
#include <iostream>

using namespace kio;

KineticIoSingleton::KineticIoSingleton() : dataCache(0), threadPool(0, 0)
{
  configuration.readahead_window_size = 0;
  try {
    loadConfiguration();
  } catch (const std::exception& e) {
    printf("Error loading configuration: %s\n", e.what());
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
  return dataCache;
}

ClusterMap& KineticIoSingleton::cmap()
{
  return clusterMap;
}

BackgroundOperationHandler& KineticIoSingleton::threadpool()
{
  return threadPool;
}

/* Utility functions for this class only. */
namespace {
/* Read file located at path into string buffer and return it. */
std::string readfile(const char* path)
{
  std::ifstream file(path);
  /* Unlimited buffer size so reading in large cluster files works. */
  file.rdbuf()->pubsetbuf(0, 0);
  std::stringstream buffer;
  buffer << file.rdbuf();
  if (buffer.str().empty()) {
    kio_error("File ", path, " could not be read in.");
    throw std::system_error(std::make_error_code(std::errc::io_error));
  }
  return buffer.str();
}

std::string loadJsonStringEntry(struct json_object* obj, const char* key)
{
  struct json_object* tmp = NULL;
  if (!json_object_object_get_ex(obj, key, &tmp)) {
    kio_error("Failed reading in key ", key);
    throw std::system_error(std::make_error_code(std::errc::invalid_argument));
  }
  return json_object_get_string(tmp);
}

int loadJsonIntEntry(struct json_object* obj, const char* key)
{
  struct json_object* tmp = NULL;
  if (!json_object_object_get_ex(obj, key, &tmp)) {
    kio_error("Failed reading in key ", key);
    throw std::system_error(std::make_error_code(std::errc::invalid_argument));
  }
  return json_object_get_int(tmp);
}

void put_json(json_object* json_root)
{
  json_object_put(json_root);
}
}

void KineticIoSingleton::loadConfiguration()
{
  const char* location = getenv("KINETIC_DRIVE_LOCATION");
  if (!location) {
    kio_error("KINETIC_DRIVE_LOCATION not set.");
    throw std::system_error(std::make_error_code(std::errc::invalid_argument));
  }

  const char* security = getenv("KINETIC_DRIVE_SECURITY");
  if (!security) {
    kio_error("KINETIC_DRIVE_SECURITY not set.");
    throw std::system_error(std::make_error_code(std::errc::invalid_argument));
  }

  const char* cluster = getenv("KINETIC_CLUSTER_DEFINITION");
  if (!cluster) {
    kio_error("KINETIC_CLUSTER_DEFINITION not set.");
    throw std::system_error(std::make_error_code(std::errc::invalid_argument));
  }

  /* If the environment variables contain the json content, use them directly, otherwise get file contents */
  std::string location_data = location[0] == '/' || location[0] == '.' ? readfile(location) : location;
  std::string security_data = security[0] == '/' || security[0] == '.' ? readfile(security) : security;
  std::string cluster_data = cluster[0] == '/' || cluster[0] == '.' ? readfile(cluster) : cluster;

  typedef std::unique_ptr<json_object, void (*)(json_object*)> unique_json_ptr;

  unique_json_ptr location_root(json_tokener_parse(location_data.c_str()), &put_json);
  if (!location_root) {
    kio_error("Failed initializing json token parser for location information."
              " KINETIC_DRIVE_LOCATION is set to ", location,
              " and location_data is set to ", location_data
    );
    throw std::system_error(std::make_error_code(std::errc::executable_format_error));
  }

  unique_json_ptr security_root(json_tokener_parse(security_data.c_str()), &put_json);
  if (!security_root) {
    kio_error("Failed initializing json token parser for security information."
              " KINETIC_DRIVE_SECURITY is set to ", security,
              " and location_data is set to ", security_data
    );
    throw std::system_error(std::make_error_code(std::errc::executable_format_error));
  }

  unique_json_ptr cluster_root(json_tokener_parse(cluster_data.c_str()), &put_json);
  if (!cluster_root) {
    kio_error("Failed initializing json token parser for security information."
                  " KINETIC_CLUSTER_DEFINITION is set to ", cluster,
                  " and location_data is set to ", cluster_data
    );
    throw std::system_error(std::make_error_code(std::errc::executable_format_error));
  }

  struct json_object* o1 = NULL;
  struct json_object* o2 = NULL;

  if (!json_object_object_get_ex(location_root.get(), "location", &o1)) {
    kio_error("No location entry found");
    throw std::system_error(std::make_error_code(std::errc::invalid_argument));
  }
  if (!json_object_object_get_ex(security_root.get(), "security", &o2)) {
    kio_error("No security entry found");
    throw std::system_error(std::make_error_code(std::errc::invalid_argument));
  }
  auto driveInfo = parseDrives(o1, o2);

  if (!json_object_object_get_ex(cluster_root.get(), "cluster", &o1)) {
    kio_error("No cluster entry found");
    throw std::system_error(std::make_error_code(std::errc::invalid_argument));
  };
  auto clusterInfo = parseClusters(o1);

  if (!json_object_object_get_ex(cluster_root.get(), "configuration", &o1)) {
    kio_error("No configuration entry found");
    throw std::system_error(std::make_error_code(std::errc::invalid_argument));
  };
  parseConfiguration(o1);

  std::lock_guard<std::mutex> lock(mutex);
  clusterMap.reset(std::move(clusterInfo), std::move(driveInfo));
  dataCache.changeConfiguration(configuration.stripecache_capacity);
  threadPool.changeConfiguration(configuration.background_io_threads, configuration.background_io_queue_capacity);
}

std::unordered_map<std::string, std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>> KineticIoSingleton::parseDrives(
    struct json_object* locations, struct json_object* security)
{
  std::unordered_map<std::string, std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>> driveInfo;

  struct json_object* host = NULL;
  struct json_object* tmp = NULL;

  int length = json_object_array_length(locations);
  for (int i = 0; i < length; i++) {
    auto drive = json_object_array_get_idx(locations, i);
    auto id = loadJsonStringEntry(drive, "wwn");

    if (!json_object_object_get_ex(drive, "inet4", &tmp)) {
      kio_error("Drive with wwn ", id, " is missing location information");
      throw std::system_error(std::make_error_code(std::errc::invalid_argument));
    }

    host = json_object_array_get_idx(tmp, 0);
    if (!host) {
      kio_error("Drive with wwn ", id, " is missing location information");
      throw std::system_error(std::make_error_code(std::errc::invalid_argument));
    };
    std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions> kops;
    kops.first.host = json_object_get_string(host);

    host = json_object_array_get_idx(tmp, 1);
    if (host) {
      kops.second.host = json_object_get_string(host);
    }
    else {
      kops.second.host = kops.first.host;
    }

    kops.first.port = kops.second.port = loadJsonIntEntry(drive, "port");
    kops.first.use_ssl = kops.second.use_ssl = false;
    driveInfo.insert(std::make_pair(id, kops));
  }

  length = json_object_array_length(security);
  for (int i = 0; i < length; i++) {
    auto drive = json_object_array_get_idx(security, i);
    auto id = loadJsonStringEntry(drive, "wwn");
    /* Require that drive info has been scanned already.*/
    if (!driveInfo.count(id)) {
      kio_error("Security for unknown drive with wwn ", id, " provided.");
      throw std::system_error(std::make_error_code(std::errc::invalid_argument));
    };

    auto& kops = driveInfo.at(id);
    kops.first.user_id = kops.second.user_id = loadJsonIntEntry(drive, "userId");
    kops.first.hmac_key = kops.second.hmac_key = loadJsonStringEntry(drive, "key");
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

    cinfo.numData = (size_t) loadJsonIntEntry(cluster, "numData");
    cinfo.numParity = (size_t) loadJsonIntEntry(cluster, "numParity");

    cinfo.blockSize = (size_t) loadJsonIntEntry(cluster, "chunkSizeKB");
    cinfo.blockSize *= 1024;

    cinfo.min_reconnect_interval = std::chrono::seconds(loadJsonIntEntry(cluster, "minReconnectInterval"));
    cinfo.operation_timeout = std::chrono::seconds(loadJsonIntEntry(cluster, "timeout"));

    struct json_object* list = NULL;
    if (!json_object_object_get_ex(cluster, "drives", &list)) {
      kio_error("Could not find drive list for cluster ", id);
      throw std::system_error(std::make_error_code(std::errc::invalid_argument));
    }

    json_object* drive = NULL;
    int num_drives = json_object_array_length(list);

    for (int j = 0; j < num_drives; j++) {
      drive = json_object_array_get_idx(list, j);
      cinfo.drives.push_back(loadJsonStringEntry(drive, "wwn"));
    }
    clusterInfo.insert(std::make_pair(id, cinfo));
  }
  return clusterInfo;
}

void KineticIoSingleton::parseConfiguration(struct json_object* config)
{
  configuration.stripecache_capacity = (size_t) loadJsonIntEntry(config, "cacheCapacityMB");
  configuration.stripecache_capacity *= 1024 * 1024;

  configuration.readahead_window_size = (size_t) loadJsonIntEntry(config, "maxReadaheadWindow");
  configuration.background_io_threads = loadJsonIntEntry(config, "maxBackgroundIoThreads");
  configuration.background_io_queue_capacity = loadJsonIntEntry(config, "maxBackgroundIoQueue");
}

size_t KineticIoSingleton::readaheadWindowSize()
{
  return configuration.readahead_window_size;
}

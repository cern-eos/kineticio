#include <stdlib.h>
#include <fstream>
#include <sstream>
#include "KineticClusterMap.hh"
#include "KineticSingletonCluster.hh"
#include "KineticException.hh"
#include <stdio.h>


/* Read file located at path into string buffer and return it. */
static std::string readfile(const char* path)
{
  std::ifstream file(path);
  /* Unlimted buffer size so reading in large cluster files works. */
  file.rdbuf()->pubsetbuf(0, 0);
  std::stringstream buffer;
  buffer << file.rdbuf();
  return buffer.str();
}

/* Printing errors initializing static global object to stderr.*/
KineticClusterMap::KineticClusterMap()
{
  /* get file names */
  const char* location = getenv("KINETIC_DRIVE_LOCATION");
  if(!location){
    fprintf(stderr,"KINETIC_DRIVE_LOCATION not set.\n");
    return;
  }
  const char* security = getenv("KINETIC_DRIVE_SECURITY");
  if(!security){
    fprintf(stderr,"KINETIC_DRIVE_SECURITY not set.\n");
    return;
  }

  /* get file contents */
  std::string location_data = readfile(location);
  if(location_data.empty()){
    fprintf(stderr,"File '%s' could not be read in.\n",location);
    return;
  }

  std::string security_data = readfile(security);
  if(security_data.empty()){
    fprintf(stderr,"File '%s' could not be read in.\n",security);
    return;
  }

  /* parse files */
  if(parseJson(location_data, filetype::location)){
    fprintf(stderr,"Error while parsing location json file '%s\n",location);
    return;
  }
  if(parseJson(security_data, filetype::security)){
    fprintf(stderr,"Error while parsing security json file '%s\n",security);
    return;
  }
}

KineticClusterMap::~KineticClusterMap()
{
}

std::shared_ptr<KineticClusterInterface>  KineticClusterMap::getCluster(const std::string & id)
{
  std::unique_lock<std::mutex> locker(mutex);

  if(!map.count(id))
    throw KineticException(ENODEV,__FUNCTION__,__FILE__,__LINE__,"Nonexisting "
        "cluster id '"+id+"' requested.");

  KineticClusterInfo & ki = map.at(id);
  if(!ki.cluster)
    ki.cluster.reset(new KineticSingletonCluster(ki.connection_options));
  return ki.cluster;
}

int KineticClusterMap::getSize()
{
  return map.size();
}

int KineticClusterMap::parseDriveInfo(struct json_object * drive)
{
  struct json_object *tmp = NULL;
  KineticClusterInfo ki;
  /* We could go with wwn instead of serial number. Chosen SN since it is also
   * unique and is both shorter and contains no spaces (eos does not like spaces
   * in the path name). */
  if(!json_object_object_get_ex(drive, "serialNumber", &tmp))
    return -EINVAL;
  std::string id = json_object_get_string(tmp);

  if(!json_object_object_get_ex(drive, "inet4", &tmp))
    return -EINVAL;
  tmp = json_object_array_get_idx(tmp, 0);
  ki.connection_options.host = json_object_get_string(tmp);

  if(!json_object_object_get_ex(drive, "port", &tmp))
    return -EINVAL;
  ki.connection_options.port = json_object_get_int(tmp);

  ki.connection_options.use_ssl = false;
  map.insert(std::make_pair(id, ki));
  return 0;
}

int KineticClusterMap::parseDriveSecurity(struct json_object * drive)
{
  struct json_object *tmp = NULL;
  if(!json_object_object_get_ex(drive, "serialNumber", &tmp))
    return -EINVAL;
  std::string id = json_object_get_string(tmp);

  /* Require that drive info has been scanned already.*/
  if(!map.count(id))
      return -ENODEV;

  KineticClusterInfo & ki = map.at(id);

  if(!json_object_object_get_ex(drive, "userId", &tmp))
    return -EINVAL;
  ki.connection_options.user_id = json_object_get_int(tmp);

  if(!json_object_object_get_ex(drive, "key", &tmp))
    return -EINVAL;
  ki.connection_options.hmac_key = json_object_get_string(tmp);

  return 0;
}


int KineticClusterMap::parseJson(const std::string& filedata, filetype type)
{
  struct json_object *root = json_tokener_parse(filedata.c_str());
  if(!root)
      return -EINVAL;

  struct json_object *d = NULL;
  struct json_object *dlist = NULL;

  if(!json_object_object_get_ex(root,
          type == filetype::location ? "location" : "security", &dlist))
    return -EINVAL;

  int num_drives = json_object_array_length(dlist);
  int err = 0;
  for(int i=0; i<num_drives; i++){
    d = json_object_array_get_idx(dlist, i);
    if(type == filetype::location)
      err = parseDriveInfo(d);
    else if (type == filetype::security)
      err = parseDriveSecurity(d);
    if(err) return err;
  }
  return 0;
}
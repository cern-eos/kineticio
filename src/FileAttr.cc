#include "FileAttr.hh"
#include "Utility.hh"

using std::string;
using namespace kio;

FileAttr::FileAttr(std::string base,
                   std::shared_ptr<ClusterInterface> c) :
    path(base), cluster(c)
{
}

FileAttr::~FileAttr()
{
}

enum class RequestType {
  STANDARD, READ_OPS, READ_BW, WRITE_OPS, WRITE_BW, MAX_BW
};

RequestType getRequestType(const char *name)
{
  if(strcmp(name, "sys.iostats.max-bw") == 0)
    return RequestType::MAX_BW;
  if(strcmp(name, "sys.iostats.read-bw") == 0)
    return RequestType::READ_BW;
  if(strcmp(name, "sys.iostats.read-ops") == 0)
    return RequestType::READ_OPS;
  if(strcmp(name, "sys.iostats.write-bw") == 0)
    return RequestType::WRITE_BW;
  if(strcmp(name, "sys.iostats.write-ops") == 0)
    return RequestType::WRITE_OPS;
  return RequestType::STANDARD; 
}

bool FileAttr::Get(const char *name, char *content, size_t &size)
{
  if (!cluster)
    return false;
  
  auto type = getRequestType(name); 
  std::shared_ptr<const string> value;
  
  if(type == RequestType::STANDARD){
    std::shared_ptr<const string> version;
    auto status = cluster->get(
        utility::makeAttributeKey(cluster->id(), path, name),
        false, version, value);

    /* Requested attribute doesn't exist or there was connection problem. */
    if (!status.ok())
      return false;
  }else{  
    auto stats = cluster->iostats();
    double MB = 1024*1024;
    double result = 0;
    switch(type){
      case RequestType::MAX_BW: 
        result = stats.number_drives*50*MB;
        break;
      case RequestType::READ_BW:
        result = stats.read_bytes/MB;
        break;
      case RequestType::READ_OPS:
        result = stats.read_ops;
        break;
      case RequestType::WRITE_BW: 
        result = stats.write_bytes/MB;
        break;
      case RequestType::WRITE_OPS:
        result = stats.write_ops;
        break; 
    }
    value = std::make_shared<const string>(utility::Convert::toString(result));
  }
  
  if (size > value->size())
    size = value->size();
  value->copy(content, size, 0);
  return true;
}

string FileAttr::Get(string name)
{
  char buf[1024];
  size_t size = sizeof(buf);
  if (Get(name.c_str(), buf, size))
    return string(buf, size);
  return string("");
}

bool FileAttr::Set(const char *name, const char *content, size_t size)
{
  if (!cluster)
    return false;

  auto empty = std::make_shared<const string>();
  auto status = cluster->put(
      utility::makeAttributeKey(cluster->id(), path, name),
      empty,
      std::make_shared<const string>(content, size),
      true,
      empty
  );
  return status.ok();
}

bool FileAttr::Set(std::string key, std::string value)
{
  return Set(key.c_str(), value.c_str(), value.size());
}

#include "FileAttr.hh"

using std::string;
using namespace kio;

FileAttr::FileAttr(const char* p,
        std::shared_ptr<ClusterInterface> c) :
             path(p), cluster(c)
{
}

FileAttr::~FileAttr()
{
}

bool FileAttr::Get(const char* name, char* content, size_t& size)
{
  if(!cluster){
      return false;
  }
  std::shared_ptr<const string> version;
  std::shared_ptr<const string> value;
  auto status = cluster->get(
          std::make_shared<const string>(path+"_attr_"+name),
          false, version, value);

  /* Requested attribute doesn't exist or there was connection problem. */
  if(!status.ok())
      return false;

  if(size > value->size())
    size = value->size();
  value->copy(content, size, 0);
  return true;
}

string FileAttr::Get(string name)
{
  char buf[1024];
  size_t size = sizeof(buf);
  if(Get(name.c_str(), buf, size) == true)
    return string(buf, size);
  return string("");
}

bool FileAttr::Set(const char * name, const char * content, size_t size)
{
  if(!cluster)
    return false;

  auto empty = std::make_shared<const string>();
  auto status = cluster->put(
          std::make_shared<const string>(path+"_attr_"+name),
          empty,
          std::make_shared<const string>(content,size), 
          true,
          empty
  );
  if(!status.ok())
    return false;
  return true;
}

bool FileAttr::Set(std::string key, std::string value)
{
  return Set(key.c_str(), value.c_str(), value.size());
}

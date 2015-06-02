#include "KineticFileAttr.hh"
#include "KineticClusterMap.hh"
#include "PathUtil.hh"

using std::string;
using std::shared_ptr;
using std::make_shared;
using kinetic::KineticStatus;
using kinetic::StatusCode;

KineticFileAttr::KineticFileAttr(const char* p,
        std::shared_ptr<KineticClusterInterface> c) :
             path(p), cluster(c)
{
}

KineticFileAttr::~KineticFileAttr()
{
}

bool KineticFileAttr::Get(const char* name, char* content, size_t& size)
{
  if(!cluster){
      return false;
  }
  shared_ptr<const string> version;
  shared_ptr<const string> value;
  auto status = cluster->get(
          make_shared<const string>(path+"_attr_"+name),
          false, version, value);

  /* Requested attribute doesn't exist or there was connection problem. */
  if(!status.ok())
      return false;

  if(size > value->size())
    size = value->size();
  value->copy(content, size, 0);
  return true;
}

string KineticFileAttr::Get(string name)
{
  char buf[1024];
  size_t size = sizeof(buf);
  if(Get(name.c_str(), buf, size) == true)
    return string(buf, size);
  return string("");
}

bool KineticFileAttr::Set(const char * name, const char * content, size_t size)
{
  if(!cluster)
    return false;

  auto empty = make_shared<const string>();
  auto status = cluster->put(
          make_shared<const string>(path+"_attr_"+name),
          empty, make_shared<const string>(string(content,size)), true, empty);
  if(!status.ok())
    return false;
  return true;
}

bool KineticFileAttr::Set(std::string key, std::string value)
{
  return Set(key.c_str(), value.c_str(), value.size());
}

/* As in Attr.cc implementation, ensure that the file exists
 * in static OpenAttr function. */
KineticFileAttr* KineticFileAttr::OpenAttr (const char* path)
{
  auto cluster = cmap().getCluster(path_util::extractID(path));

  shared_ptr<const string> empty;
  auto status = cluster->get(make_shared<const string>(path), true, empty, empty);

  if(!status.ok())
    return 0;

  return new KineticFileAttr(path, cluster);
}

KineticFileAttr* KineticFileAttr::OpenAttribute (const char* path)
{
  return OpenAttr(path);
}
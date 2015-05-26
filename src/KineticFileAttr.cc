#include "KineticFileAttr.hh"

#define eos_debug //printf
#define eos_info //printf
#define eos_warning //printf
#define eos_err //printf

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
  shared_ptr<const string> key(new string(path+"_attr_"+name));
  shared_ptr<const string> version(new string());
  shared_ptr<string> value(new string());
  KineticStatus status = cluster->get(key, version, value, false);

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

  shared_ptr<const string> key(new string(path+"_attr_"+name));
  shared_ptr<const string> version(new string());
  shared_ptr<string> value(new string(content, size));

  KineticStatus status = cluster->put(key, version, value, true);
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
  auto c = cmap().getCluster(path_util::extractID(path)); 

  shared_ptr<const string> version(new string());
  shared_ptr<string> value(new string());
  if(!c->get(make_shared<const string>(path), version, value, true).ok())
    return 0;

  return new KineticFileAttr(path, c);
}

KineticFileAttr* KineticFileAttr::OpenAttribute (const char* path)
{
  return OpenAttr(path);
}
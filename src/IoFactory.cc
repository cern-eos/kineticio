#include "IoFactory.hh"
#include "FileIo.hh"
#include "FileAttr.hh"
#include "ClusterMap.hh"
#include "PathUtil.hh"


std::shared_ptr<FileIoInterface> IoFactory::sharedFileIo()
{
  return std::make_shared<FileIo>();
}

std::unique_ptr<FileIoInterface> IoFactory::uniqueFileIo()
{
  return std::unique_ptr<FileIoInterface>(new FileIo());
}


std::shared_ptr<ClusterInterface> attrCluster(const char* path)
{
  auto cluster = cmap().getCluster(path_util::extractID(path));

  std::shared_ptr<const std::string> empty;
  auto status = cluster->get(std::make_shared<const std::string>(path), true, empty, empty);

  if(!status.ok())
    return std::shared_ptr<ClusterInterface>();
  
  return cluster; 
}


std::shared_ptr<FileAttrInterface> IoFactory::sharedFileAttr(const char* path)
{
  auto cluster = attrCluster(path);
  if(cluster)
    return std::make_shared<FileAttr>(path, cluster);
  return std::shared_ptr<FileAttrInterface>();
}

std::unique_ptr<FileAttrInterface> IoFactory::uniqueFileAttr(const char* path)
{
  auto cluster = attrCluster(path);
  if(cluster)
    return std::unique_ptr<FileAttr>(new FileAttr(path, cluster));
  return std::unique_ptr<FileAttrInterface>();
}
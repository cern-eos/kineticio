#include "KineticIoFactory.hh"
#include "FileIo.hh"
#include "FileAttr.hh"
#include "ClusterMap.hh"
#include "Utility.hh"

using namespace kio;


std::unique_ptr<FileIoInterface> Factory::uniqueFileIo()
{
  return std::unique_ptr<FileIoInterface>(new FileIo());
}

std::unique_ptr<FileAttrInterface> Factory::uniqueFileAttr(const char *path)
{
  auto cluster = ClusterMap::getInstance().getCluster(utility::extractClusterID(path));

  std::shared_ptr<const std::string> empty;
  auto status = cluster->get(std::make_shared<const std::string>(path), true, empty, empty);

  if (!status.ok())
    return std::unique_ptr<FileAttrInterface>();
  return std::unique_ptr<FileAttr>(new FileAttr(path, cluster));
}
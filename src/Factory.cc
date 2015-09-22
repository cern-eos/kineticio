#include "KineticIoFactory.hh"
#include "FileIo.hh"
#include "FileAttr.hh"
#include "ClusterMap.hh"
#include "Utility.hh"
#include "Logging.hh"

using namespace kio;


std::unique_ptr<FileIoInterface> Factory::makeFileIo()
{
  return std::unique_ptr<FileIoInterface>(new FileIo());
}

std::unique_ptr<FileAttrInterface> Factory::makeFileAttr(const char* path)
{
  auto cluster = ClusterMap::getInstance().getCluster(utility::extractClusterID(path));

  std::shared_ptr<const std::string> empty;
  auto status = cluster->get(std::make_shared<const std::string>(path), true, empty, empty);

  if (!status.ok())
    return std::unique_ptr<FileAttrInterface>();
  return std::unique_ptr<FileAttr>(new FileAttr(path, cluster));
}

void Factory::registerLogFunction(logfunc_t log, shouldlogfunc_t shouldLog)
{
  Logger::get().registerLogFunction(std::move(log), std::move(shouldLog));
}

std::unique_ptr<KineticAdminClusterInterface> Factory::makeAdminCluster(const char* cluster_id)
{
  return ClusterMap::getInstance().getAdminCluster(cluster_id);
}
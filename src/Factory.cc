#include "KineticIoFactory.hh"
#include "FileIo.hh"
#include "FileAttr.hh"
#include "KineticIoSingleton.hh"
#include "Utility.hh"
#include "Logging.hh"

using namespace kio;


std::unique_ptr<FileIoInterface> Factory::makeFileIo()
{
  return std::unique_ptr<FileIoInterface>(new FileIo());
}

std::unique_ptr<FileAttrInterface> Factory::makeFileAttr(const char* path)
{
  auto clusterId = utility::extractClusterID(path);
  auto cluster = kio().cmap().getCluster(clusterId);
  auto base = utility::extractBasePath(path);
  auto mdkey = utility::makeMetadataKey(clusterId, base);
  
  std::shared_ptr<const std::string> empty;
  auto status = cluster->get(mdkey, true, empty, empty);

  if (!status.ok())
    return std::unique_ptr<FileAttrInterface>();
  return std::unique_ptr<FileAttr>(new FileAttr(base, cluster));
}

void Factory::registerLogFunction(logfunc_t log, shouldlogfunc_t shouldLog)
{
  Logger::get().registerLogFunction(std::move(log), std::move(shouldLog));
}

std::unique_ptr<AdminClusterInterface> Factory::makeAdminCluster(const char* cluster_id)
{
  return kio().cmap().getAdminCluster(cluster_id);
}

void Factory::reloadConfiguration()
{
  kio().loadConfiguration();
}
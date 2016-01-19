#include "KineticIoFactory.hh"
#include "FileIo.hh"
#include "KineticIoSingleton.hh"
#include "Utility.hh"
#include "Logging.hh"

using namespace kio;


std::unique_ptr<FileIoInterface> KineticIoFactory::makeFileIo(const std::string& path)
{
  return std::unique_ptr<FileIoInterface>(new FileIo(path));
}

void KineticIoFactory::registerLogFunction(logfunc_t log, shouldlogfunc_t shouldLog)
{
  Logger::get().registerLogFunction(std::move(log), std::move(shouldLog));
}

std::unique_ptr<AdminClusterInterface> KineticIoFactory::makeAdminCluster(const std::string& cluster_id, RedundancyType redundancy)
{
  return kio().cmap().getAdminCluster(cluster_id, redundancy);
}

void KineticIoFactory::reloadConfiguration()
{
  kio().loadConfiguration();
}
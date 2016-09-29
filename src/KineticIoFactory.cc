/************************************************************************
 * KineticIo - a file io interface library to kinetic devices.          *
 *                                                                      *
 * This Source Code Form is subject to the terms of the Mozilla         *
 * Public License, v. 2.0. If a copy of the MPL was not                 *
 * distributed with this file, You can obtain one at                    *
 * https://mozilla.org/MP:/2.0/.                                        *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but is provided AS-IS, WITHOUT ANY WARRANTY; including without       *
 * the implied warranty of MERCHANTABILITY, NON-INFRINGEMENT or         *
 * FITNESS FOR A PARTICULAR PURPOSE. See the Mozilla Public             *
 * License for more details.                                            *
 ************************************************************************/

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
  kio_notice("Registered log function. Library Version = ", KIO_VERSION);
}

std::shared_ptr<AdminClusterInterface> KineticIoFactory::makeAdminCluster(const std::string& cluster_id)
{
  return kio().cmap().getAdminCluster(cluster_id);
}

void KineticIoFactory::reloadConfiguration()
{
  kio().loadConfiguration();
}

namespace kio {
class LoadableKineticIoFactory : public LoadableKineticIoFactoryInterface
{
  std::unique_ptr<FileIoInterface> makeFileIo(const std::string& path)
  {
    return KineticIoFactory::makeFileIo(path);
  }

  std::shared_ptr<AdminClusterInterface> makeAdminCluster(const std::string& cluster_id)
  {
    return KineticIoFactory::makeAdminCluster(cluster_id);
  }

  void registerLogFunction(logfunc_t log, shouldlogfunc_t shouldLog)
  {
    return KineticIoFactory::registerLogFunction(log, shouldLog);
  }

  void reloadConfiguration()
  {
    return KineticIoFactory::reloadConfiguration();
  }
};
}

extern "C" kio::LoadableKineticIoFactoryInterface* getKineticIoFactory()
{
  static kio::LoadableKineticIoFactory loadable_factory;
  return &loadable_factory;
};
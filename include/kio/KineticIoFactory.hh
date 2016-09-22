//------------------------------------------------------------------------------
//! @file KineticIoFactory.hh
//! @author Paul Hermann Lensing
//! @brief Factory class for public KineticIo objects.
//------------------------------------------------------------------------------

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

#ifndef __KINETICIO_FACTORY_HH__
#define  __KINETICIO_FACTORY_HH__

#include <memory>
#include "FileIoInterface.hh"
#include "AdminClusterInterface.hh"

namespace kio {
typedef std::function<void(const char* func, const char* file, int line, int level, const char* msg)> logfunc_t;
typedef std::function<bool(const char* func, int level)> shouldlogfunc_t;

//----------------------------------------------------------------------------
//! The only way for clients of the public library interface to construct
//! FileIo objects. Returns unique_ptr as the caller will
//! have exclusive ownership (if wanted, caller can transfer ownership to
//! shared_ptr himself). Additionally, some administrative functionality
//! is supplied here.
//----------------------------------------------------------------------------
class KineticIoFactory {
public:
  //--------------------------------------------------------------------------
  //! Construct a FileIo object and return it in a unique pointer.
  //!
  //! @return unique pointer to constructed FileIo object.
  //--------------------------------------------------------------------------
  static std::unique_ptr<FileIoInterface> makeFileIo(const std::string& path);

  //--------------------------------------------------------------------------
  //! Returns the internal cluster object, accessable through the admin
  //! interface. Does not generate a new cluster object, which is why ownership
  //! is shared.
  //!
  //! @param cluster_id the id of the cluster
  //! @return shared pointer to constructed AdminCluster object
  //--------------------------------------------------------------------------
  static std::shared_ptr<AdminClusterInterface> makeAdminCluster(const std::string& cluster_id);

  //--------------------------------------------------------------------------
  //! The client may register a log function that will be used for debug and
  //! warning messages in the library. Fatal error messages will continue to
  //! be thrown. Registered function may change at any time.
  //!
  //! @param log the log function to be used by the library
  //! @param shouldLog function to query if a specific loglevel should be logged
  //--------------------------------------------------------------------------
  static void registerLogFunction(logfunc_t log, shouldlogfunc_t shouldLog);

  //--------------------------------------------------------------------------
  //! The client may force a configuration re-load if the contents
  //! of the JSON configuration files have changed.
  //--------------------------------------------------------------------------
  static void reloadConfiguration();
};

//----------------------------------------------------------------------------
//! To make the factory usable when loading the kineticio library
//! dynamically via dl_open, an abstract class interface is provided.
//! Using it really doesn't make sense when linking kineticio normally.
//----------------------------------------------------------------------------
class LoadableKineticIoFactoryInterface
{
public:
  //! See KineticIoFactory
  virtual std::unique_ptr<FileIoInterface> makeFileIo(const std::string& path) = 0;

  //! See KineticIoFactory
  virtual std::shared_ptr<AdminClusterInterface> makeAdminCluster(const std::string& cluster_id) = 0;

  //! See KineticIoFactory
  virtual void registerLogFunction(logfunc_t log, shouldlogfunc_t shouldLog) = 0;

  //! See KineticIoFactory
  virtual void reloadConfiguration() = 0;

  virtual ~LoadableKineticIoFactoryInterface()
  {};
};

//----------------------------------------------------------------------------
//! Factory creation method. Only usefull for dynamically loading the
//! kineticio library.
//----------------------------------------------------------------------------
extern "C" LoadableKineticIoFactoryInterface* createKineticIoFactory;
//----------------------------------------------------------------------------
//! Factory destruction method. Only usefull for dynamically loading the
//! kineticio library.
//----------------------------------------------------------------------------
extern "C" void destroyKineticIoFactory(kio::LoadableKineticIoFactoryInterface* ioFactory);

}

#endif	/* __KINETICIO_FACTORY_HH__ */


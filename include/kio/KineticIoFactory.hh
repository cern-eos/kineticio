//------------------------------------------------------------------------------
//! @file KineticIoFactory.hh
//! @author Paul Hermann Lensing
//! @brief Factory class for FileIo and FileAttr objects.
//------------------------------------------------------------------------------
#ifndef __KINETICIO_FACTORY_HH__
#define	__KINETICIO_FACTORY_HH__

#include <memory>
#include "FileIoInterface.hh"
#include "FileAttrInterface.hh"
#include "AdminClusterInterface.hh"

namespace kio{
  typedef std::function<void(const char* func, const char* file, int line, int level, const char* msg)> logfunc_t;
  typedef std::function<bool(const char* func, int level)> shouldlogfunc_t;

  //----------------------------------------------------------------------------
  //! The only way for clients of the public library interface to construct
  //! FileIo and FileAttr objects. Returns unique_ptr as the caller will
  //! have exclusive ownership (if wanted, caller can transfer ownership to
  //! shared_ptr himself).
  //----------------------------------------------------------------------------
  class Factory{
  public:
    //--------------------------------------------------------------------------
    //! Construct a FileIo object and return it in a unique pointer.
    //!
    //! @return unique pointer to constructed FileIo object.
    //--------------------------------------------------------------------------
    static std::unique_ptr<FileIoInterface> makeFileIo();

    //--------------------------------------------------------------------------
    //! Construct a FileAttr object and return it in a unique pointer.
    //!
    //! @param path the eos path to the FileIo object this attribute is associated with
    //! @return unique pointer to constructed FileAttr object.
    //--------------------------------------------------------------------------
    static std::unique_ptr<FileAttrInterface> makeFileAttr(const char* path);


    //--------------------------------------------------------------------------
    //! Construct an AdminCluster object and return it in a unique pointer.
    //!
    //! @param cluster_id the id of the cluster
    //! @return unique pointer to constructed AdminCluster object.
    //--------------------------------------------------------------------------
    static std::unique_ptr<KineticAdminClusterInterface> makeAdminCluster(const char* cluster_id);

    //--------------------------------------------------------------------------
    //! The client may register a log function that will be used for debug and
    //! warning messages in the library. Fatal error messages will continue to
    //! be thrown. Registered function may change at any time.
    //!
    //! @param log the log function to be used by the library
    //! @param shouldLog function to query if a specific loglevel should be logged
    //--------------------------------------------------------------------------
    static void registerLogFunction(logfunc_t log, shouldlogfunc_t shouldLog);



  };
}

#endif	/* __KINETICIO_FACTORY_HH__ */


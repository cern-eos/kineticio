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

namespace kio{

  //----------------------------------------------------------------------------
  //! The only way for clients of the public library interface to construct
  //! FileIo and FileAttr objects. Returns unique_ptr as the caller will
  //! have exclusive ownership (if wanted, caller can transfer ownership to
  //! shared_ptr himself. )
  //----------------------------------------------------------------------------
  class Factory{
  public:
    //--------------------------------------------------------------------------
    //! Construct a FileIo object and return it in a unique pointer.
    //!
    //! @return unique pointer to constructed FileIo object.
    //--------------------------------------------------------------------------
    static std::unique_ptr<FileIoInterface> uniqueFileIo();

    //--------------------------------------------------------------------------
    //! Construct a FileAttr object and return it in a unique pointer.
    //!
    //! @path the eos path to the FileIo object this attribute is associated with
    //! @return unique pointer to constructed FileAttr object.
    //--------------------------------------------------------------------------
    static std::unique_ptr<FileAttrInterface> uniqueFileAttr(const char* path);
  };
}

#endif	/* __KINETICIO_FACTORY_HH__ */

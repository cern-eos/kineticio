//------------------------------------------------------------------------------
//! @file Factory.hh
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
  //! FileIo and FileAttr objects.
  //----------------------------------------------------------------------------
  class Factory{
  public:
    //--------------------------------------------------------------------------
    //! Construct a FileIo object and store it in a shared pointer.
    //!
    //! @return shared pointer to constructed FileIo object.
    //--------------------------------------------------------------------------
    static std::shared_ptr<FileIoInterface> sharedFileIo();

    //--------------------------------------------------------------------------
    //! Construct a FileIo object and store it in a unique pointer.
    //!
    //! @return unique pointer to constructed FileIo object.
    //--------------------------------------------------------------------------
    static std::unique_ptr<FileIoInterface> uniqueFileIo();

    //--------------------------------------------------------------------------
    //! Construct a FileAttr object and store it in a shared pointer.
    //!
    //! @path the eos path to the FileIo object this attribute is associated with
    //! @return shared pointer to constructed FileAttr object.
    //--------------------------------------------------------------------------
    static std::shared_ptr<FileAttrInterface> sharedFileAttr(const char* path);

    //--------------------------------------------------------------------------
    //! Construct a FileAttr object and store it in a unique pointer.
    //!
    //! @path the eos path to the FileIo object this attribute is associated with
    //! @return unique pointer to constructed FileAttr object.
    //--------------------------------------------------------------------------
    static std::unique_ptr<FileAttrInterface> uniqueFileAttr(const char* path);
  };
}

#endif	/* __KINETICIO_FACTORY_HH__ */


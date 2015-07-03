//------------------------------------------------------------------------------
//! @file FileAttrInterface.hh
//! @author Paul Hermann Lensing
//! @brief EOS style attribute functionality.
//------------------------------------------------------------------------------
#ifndef __KINETICIO_FILEATTRINTERFACE__HH__
#define __KINETICIO_FILEATTRINTERFACE__HH__

#include <string>

//------------------------------------------------------------------------------
//! Interface used for doing Kinetic attribute IO operations,
//! mirroring EOS FileIo::Attr interface.
//------------------------------------------------------------------------------
class FileAttrInterface{

public:
  // ------------------------------------------------------------------------
  //! Set a binary attribute (name has to start with 'user.' !!!)
  // ------------------------------------------------------------------------
  virtual bool Set (const char* name, const char* value, size_t len) = 0;

  // ------------------------------------------------------------------------
  //! Set a string attribute (name has to start with 'user.' !!!)
  // ------------------------------------------------------------------------
  virtual bool Set (std::string key, std::string value) = 0;

  // ------------------------------------------------------------------------
  //! Get a binary attribute by name (name has to start with 'user.' !!!)
  // ------------------------------------------------------------------------
  virtual bool Get (const char* name, char* value, size_t &size) = 0;

  // ------------------------------------------------------------------------
  //! Get a string attribute by name (name has to start with 'user.' !!!)
  // ------------------------------------------------------------------------
  virtual std::string Get (std::string name) = 0;

  // ------------------------------------------------------------------------
  // Destructor
  // ------------------------------------------------------------------------
  virtual ~FileAttrInterface (){};
};


#endif // __KINETICIO_FILEATTRINTERFACE__HH__
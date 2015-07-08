//------------------------------------------------------------------------------
//! @file FileAttr.hh
//! @author Paul Hermann Lensing
//! @brief EOS style attribute functionality.
//------------------------------------------------------------------------------
#ifndef __FILEATTR__HH__
#define __FILEATTR__HH__

#include "ClusterInterface.hh"
#include "FileAttrInterface.hh"
#include <string>
#include <memory>

namespace kio{

//------------------------------------------------------------------------------
//! Class used for doing kio attribute IO operations, mirroring FileIo::Attr
//! interface. Can throw.
//------------------------------------------------------------------------------
class FileAttr : public FileAttrInterface {
public:
  // ------------------------------------------------------------------------
  //! Set a binary attribute (name has to start with 'user.' !!!)
  // ------------------------------------------------------------------------
  bool Set (const char* name, const char* value, size_t len);

  // ------------------------------------------------------------------------
  //! Set a string attribute (name has to start with 'user.' !!!)
  // ------------------------------------------------------------------------
  bool Set (std::string key, std::string value);

  // ------------------------------------------------------------------------
  //! Get a binary attribute by name (name has to start with 'user.' !!!)
  // ------------------------------------------------------------------------
  bool Get (const char* name, char* value, size_t &size);

  // ------------------------------------------------------------------------
  //! Get a string attribute by name (name has to start with 'user.' !!!)
  // ------------------------------------------------------------------------
  std::string Get (std::string name);

  // ------------------------------------------------------------------------
  //! Constructor
  // ------------------------------------------------------------------------
  explicit FileAttr (const char* path,
                  std::shared_ptr<ClusterInterface> cluster);

  // ------------------------------------------------------------------------
  //! Destructor
  // ------------------------------------------------------------------------
  ~FileAttr ();

private:
  //! the path of the FileIo object this attribute object is associated with
  std::string path;
  //! the cluster attributes are to be stored in / retrieved from
  std::shared_ptr<ClusterInterface> cluster;
};
}

#endif // __FILEATTR__HH__
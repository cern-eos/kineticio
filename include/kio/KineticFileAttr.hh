//------------------------------------------------------------------------------
//! @file KineticFileAttr.hh
//! @author Paul Hermann Lensing
//! @brief EOS style attribute functionality.
//------------------------------------------------------------------------------
#ifndef __KINETICFILEATTR__HH__
#define __KINETICFILEATTR__HH__

#include "KineticClusterInterface.hh"
#include <string>
#include <memory>

//------------------------------------------------------------------------------
//! Class used for doing Kinetic attribute IO operations, mirroring FileIo::Attr
//! interface. Can throw.
//------------------------------------------------------------------------------
class KineticFileAttr{
private:
  std::string path;
  std::shared_ptr<KineticClusterInterface> cluster;

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
  //! Factory function to create an attribute object
  // ------------------------------------------------------------------------
  static KineticFileAttr* OpenAttr (const char* path);

  // ------------------------------------------------------------------------
  //! Non static Factory function to create an attribute object
  // ------------------------------------------------------------------------
  KineticFileAttr* OpenAttribute (const char* path);

  // ------------------------------------------------------------------------
  // Constructor
  // ------------------------------------------------------------------------
  explicit KineticFileAttr (const char* path,
                  std::shared_ptr<KineticClusterInterface> cluster);

  // ------------------------------------------------------------------------
  // Destructor
  // ------------------------------------------------------------------------
  virtual ~KineticFileAttr ();
};

#endif // __KINETICFILEATTR__HH__
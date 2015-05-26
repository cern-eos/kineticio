//------------------------------------------------------------------------------
//! @file PathUtil.hh
//! @author Paul Hermann Lensing
//! @brief String utility functions for eos-style kinetic paths.
//------------------------------------------------------------------------------
#ifndef __PATHUTIL_HH__
#define	__PATHUTIL_HH__

/*----------------------------------------------------------------------------*/
#include <string>
#include <sstream>
#include <iomanip>

/* <cstdatomic> is part of gcc 4.4.x experimental C++0x support... <atomic> is
 * what actually made it into the standard.
#if __GNUC__ == 4 && (__GNUC_MINOR__ == 4)
    #include <cstdatomic>
#else
    #include <atomic>
#endif
 */

//------------------------------------------------------------------------------
//! String utility functions. Don't really fit in any class; used by
//! KineticFileIo and KineticFileAttr classes.
//------------------------------------------------------------------------------
namespace path_util{
  //--------------------------------------------------------------------------
  //! Create the kinetic key from the supplied path and chunk number.
  //!
  //! @param path base path
  //! @param chunk_number the chunk number
  //! @return the kinetic key for the requested chunk
  //--------------------------------------------------------------------------
  static std::shared_ptr<const std::string> chunkKey(const std::string& base,
          int chunk_number)
  {
      std::ostringstream ss;
      ss << base << "_" << std::setw(10) << std::setfill('0') << chunk_number;
      return std::make_shared<const std::string>(ss.str());
  }

  //--------------------------------------------------------------------------
  //! Extract the location id from the supplied path.
  //!
  //! @param path base path of the form kinetic:ID:path
  //! @return the location drive id
  //--------------------------------------------------------------------------
  static std::string extractID(const std::string& path)
  {
    size_t id_start = path.find_first_of(':') + 1;
    size_t id_end   = path.find_first_of(':', id_start);
    return path.substr(id_start, id_end-id_start);
  }
}



#endif	/* __PATHUTIL_HH__ */


//------------------------------------------------------------------------------
//! @file Utility.hh
//! @author Paul Hermann Lensing
//! @brief Utility functions for kineticio library.
//------------------------------------------------------------------------------
#ifndef __KINETICIO_UTILITY_HH__
#define	__KINETICIO_UTILITY_HH__

/*----------------------------------------------------------------------------*/
#include <string>
#include <sstream>
#include <kinetic/kinetic.h>

namespace kio { namespace utility {

  //--------------------------------------------------------------------------
  //! Create the kinetic key from the supplied path and chunk number.
  //!
  //! @param path base path
  //! @param chunk_number the chunk number
  //! @return the cluster key for the requested chunk
  //--------------------------------------------------------------------------
  std::shared_ptr<const std::string> constructChunkKey(const std::string& base, int chunk_number);

  //--------------------------------------------------------------------------
  //! Extract the cluster id from the supplied eos path.
  //!
  //! @param path eos kinetic path of the form kinetic:ID:path
  //! @return the extracted cluster id
  //--------------------------------------------------------------------------
  std::string extractClusterID(const std::string& path);

  //--------------------------------------------------------------------------
  //! Constructs a uuid string containing the supplied size attribute.
  //!
  //! @param size size attribute to encode in the returned uuid
  //! @return a uuid string
  //--------------------------------------------------------------------------
  std::shared_ptr<const std::string> uuidGenerateEncodeSize(std::size_t size);

  //--------------------------------------------------------------------------
  //! Decode the size attribute encoded in the supplied uuid string, which
  //! should be generated by uuidGenerateEncodeSize
  //!
  //! @param uuid the uuid string
  //! @return the size attribute encoded in the uuid string
  //--------------------------------------------------------------------------
  std::size_t uuidDecodeSize(const std::shared_ptr<const std::string>& uuid);

  //--------------------------------------------------------------------------
  //! Overloading operator<< for kinetic::StatusCode
  //!
  //! @param os the stream where a string representation of the status code
  //!   should be appended.
  //! @param c the input status code
  //--------------------------------------------------------------------------
  std::ostream& operator<<(std::ostream& os, const kinetic::StatusCode& c);

  //--------------------------------------------------------------------------
  //! Overloading operator<< for kinetic::KineticStatus. This will append
  //! both the contained status code and status message to the provided
  //! string stream.
  //!
  //! @param os the output stream
  //! @param s the input status
  //--------------------------------------------------------------------------
  std::ostream& operator<<(std::ostream& os, const kinetic::KineticStatus& s);

  //--------------------------------------------------------------------------
  //! Anything-to-string conversion, the only reason to put this in its own
  //! class is to keep the stringstream parsing methods out of the public
  //! namespace.
  //--------------------------------------------------------------------------
  class Convert{
  public:
      //--------------------------------------------------------------------------
      //! Convert an arbitrary number of arguments of variable type to a single
      //! std::string.
      //!
      //! @param args the input arguments to convert to string.
      //--------------------------------------------------------------------------
      template<typename...Args>
      static std::string toString(Args&&...args){
        std::stringstream s;
        argsToStream(s, std::forward<Args>(args)...);
        return s.str();
      }
  private:
      //--------------------------------------------------------------------------
      //! Closure of recursive parsing function
      //! @param stream string stream to store Last parameter
      //! @param last the last argument to log
      //--------------------------------------------------------------------------
      template<typename Last>
      static void argsToStream(std::stringstream& stream, Last&& last) {
        stream << last;
      }

      //--------------------------------------------------------------------------
      //! Recursive function to parse arbitrary number of variable type arguments
      //! @param stream string stream to store input parameters
      //! @param first the first of the arguments supplied to the log function
      //! @param rest the rest of the arguments should be stored in the log message
      //--------------------------------------------------------------------------
      template<typename First, typename...Rest >
      static void argsToStream(std::stringstream& stream, First&& first, Rest&&...rest) {
        stream << first;
        argsToStream(stream, std::forward<Rest>(rest)...);
      }
  };
}}

#endif	/* __KINETICIO_UTILITY_HH__ */


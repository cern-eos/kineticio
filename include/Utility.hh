//------------------------------------------------------------------------------
//! @file Utility.hh
//! @author Paul Hermann Lensing
//! @brief Utility functions for kineticio library.
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

#ifndef KINETICIO_UTILITY_HH
#define	KINETICIO_UTILITY_HH

/*----------------------------------------------------------------------------*/
#include <string>
#include <sstream>
#include <kinetic/kinetic.h>

namespace kio { namespace utility {

  //--------------------------------------------------------------------------
  //! Extract the cluster id from the supplied kinetic url.
  //!
  //! @param url kinetic url of the form kinetic://clusterId/path
  //! @return the extracted cluster id
  //--------------------------------------------------------------------------
  std::string urlToClusterId(const std::string& url);

  //--------------------------------------------------------------------------
  //! Extract the path from the supplied kinetic url
  //!
  //! @param path eos kinetic path of the form kinetic://clusterId/path
  //! @return the extracted path
  //--------------------------------------------------------------------------
  std::string urlToPath(const std::string& url);
  
  //--------------------------------------------------------------------------
  //! Create the kinetic block key from the supplied path and block number.
  //!
  //! @param path the path
  //! @param block_number the block number
  //! @return the data key for the requested block
  //--------------------------------------------------------------------------
  std::shared_ptr<const std::string> makeDataKey(const std::string& clusterId, const std::string& path, int block_number);
  
  //--------------------------------------------------------------------------
  //! Create the kinetic metadata key from the supplied path.
  //!
  //! @param path the path
  //! @param block_number the block number
  //! @return the metadata key
  //--------------------------------------------------------------------------
  std::shared_ptr<const std::string> makeMetadataKey(const std::string& clusterId, const std::string& path);
  
  //--------------------------------------------------------------------------
  //! Create the kinetic attribute key from the supplied path and name.
  //!
  //! @param clusterId the cluster id
  //! @param path the file path
  //! @param attribute_name the name of the attribute 
  //! @return the attribute key
  //--------------------------------------------------------------------------
  std::shared_ptr<const std::string> makeAttributeKey(const std::string& clusterId, const std::string& path, const std::string& attribute_name);

  //--------------------------------------------------------------------------
  //! Create the kinetic indicator key from the supplied key.
  //!
  //! @param key the data / metadata / attribute key
  //! @return the indicator key for the supplied key 
  //--------------------------------------------------------------------------
  std::shared_ptr<const std::string> makeIndicatorKey(const std::string& key);
  
  //--------------------------------------------------------------------------
  //! Obtain the original key from an indicator key
  //!
  //! @param indicator_key the indicator key 
  //! @return the orginal key 
  //--------------------------------------------------------------------------
  std::shared_ptr<const std::string> indicatorToKey(const std::string& indicator_key); 

  //--------------------------------------------------------------------------
  //! Reconstruct the url from any metadata key constructed using the utility
  //! class.
  //!
  //! @param metadata_key the key
  //! @return the kinetic url of the form kinetic://clusterId/path
  //--------------------------------------------------------------------------
  std::string metadataToUrl(const std::string& metadata_key);

  //--------------------------------------------------------------------------
  //! Extract the attribute name from any attribute key constructed using the
  //! utility class.
  //!
  //! @param key the attribute key
  //! @return the attribute name
  //--------------------------------------------------------------------------
  std::string extractAttributeName(const std::string& clusterId, const std::string& path,
                                   const std::string& attribute_key);

  //--------------------------------------------------------------------------
  //! Constructs a uuid string
  //!
  //! @return a uuid string
  //--------------------------------------------------------------------------
  std::string uuidGenerateString();

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
  //! Providing operator<< for kinetic::StatusCode
  //!
  //! @param os the stream where a string representation of the status code
  //!   should be appended.
  //! @param c the input status code
  //--------------------------------------------------------------------------
  std::ostream& operator<<(std::ostream& os, const kinetic::StatusCode& c);

  //--------------------------------------------------------------------------
  //! Providing operator<< for kinetic::KineticStatus. This will append
  //! both the contained status code and status message to the provided
  //! string stream.
  //!
  //! @param os the output stream
  //! @param s the input status code
  //--------------------------------------------------------------------------
  std::ostream& operator<<(std::ostream& os, const kinetic::KineticStatus& s);

  //--------------------------------------------------------------------------
  //! Providing operator<< for std::chrono::seconds
  //!
  //! @param os the output stream
  //! @param s the input seconds
  //--------------------------------------------------------------------------
  std::ostream& operator<<(std::ostream& os, const std::chrono::seconds& s);

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

      template<typename...Args>
      static int toInt(Args&&...args){
        std::stringstream s;
        argsToStream(s, std::forward<Args>(args)...);
        int x = 0;
        s >> x;
        return x;
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

/* Mark Adler's crc32c implementation. See crc32c.c */
extern "C" {
  uint32_t crc32c(uint32_t crc, const void* buf, size_t len);
}
#endif
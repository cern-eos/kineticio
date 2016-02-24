//------------------------------------------------------------------------------
//! @file RedundancyProvider.hh
//! @author Paul Hermann Lensing
//! @brief Class for computing parities and recovering data
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

#ifndef KINETICIO_REDUNDANCYPROVIDER_HH
#define KINETICIO_REDUNDANCYPROVIDER_HH

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <mutex>

namespace kio {

  
//------------------------------------------------------------------------------
//! The redundancy provider class offers automatic parity computing and data 
//! recovery. Depending on configuration it will use erasure coding or 
//! replication. 
//------------------------------------------------------------------------------
class RedundancyProvider {
public:
  //--------------------------------------------------------------------------
  //! Compute all missing data and parity blocks in the the stripe. Stripe size
  //! has to equal nData+nParity. Blocks can be arbitrary size, but size has
  //! to be equal within a stripe. Function will throw on incorrect input.
  //!
  //! @param stripe nData+nParity blocks, missing (empty) blocks will be
  //!   computed if possible.
  //--------------------------------------------------------------------------
  void compute(std::vector<std::shared_ptr<const std::string> >& stripe);

  //--------------------------------------------------------------------------
  //! Get nData
  //!
  //! @return the number of data blocks per stripe
  //--------------------------------------------------------------------------
  const std::size_t& numData() const;

  //--------------------------------------------------------------------------
  //! Get nParity
  //!
  //! @return the number of parity blocks per stripe
  //--------------------------------------------------------------------------
  const std::size_t& numParity() const;

  //--------------------------------------------------------------------------
  //! For convenience only, returns nData+nParity
  //!
  //! @return the number of data blocks and parity blocks per stripe
  //--------------------------------------------------------------------------
  std::size_t size() const;

  //--------------------------------------------------------------------------
  //! Constructor.
  //! Stripe parameters (number of data and parity blocks) are constant per
  //! ErasureEncoding object.
  //!
  //! @param nData number of data blocks in stripes to be encoded by this object
  //! @param nParity number of parity blocks in stripes
  //--------------------------------------------------------------------------
  explicit RedundancyProvider(std::size_t nData, std::size_t nParity);

private:
  //--------------------------------------------------------------------------
  //! Data structure to store all information required for a decode process with
  //! a known error pattern.
  //--------------------------------------------------------------------------
  struct CodingTable {
    //! the coding table
    std::vector<unsigned char> table;
    //! array of nData size, containing stripe indices to input blocks
    std::vector<unsigned int> blockIndices;
    //! Number of errors this coding table is constructed for (maximum==nParity)
    int nErrors;
  };

  //--------------------------------------------------------------------------
  //! Constructs a string of the error pattern / signature. Each missing block
  //! in the stripe is counted as an error block, existing blocks are assumed
  //! to be correct (crc integrity checks of blocks should be done previously
  //! to attempting erasure decoding).
  //!
  //! @param stripe vector of nData+nParity blocks, missing (empty) blocks are
  //!        errors
  //! @return a string of stripe size describing the error pattern
  //--------------------------------------------------------------------------
  std::string getErrorPattern(
      const std::vector<std::shared_ptr<const std::string> >& stripe
  ) const;

  //--------------------------------------------------------------------------
  //! Returns a reference to the coding table for the requested error pattern,
  //! if possible from the cache. If that particular table has not been
  //! requested before, it will be constructed.
  //!
  //! @param pattern error pattern / signature
  //! @return reference to the coding table for the supplied error pattern
  //--------------------------------------------------------------------------
  CodingTable& getCodingTable(
      const std::string& pattern
  );

private:
  //! number of data blocks in the stripe
  const std::size_t nData;
  //! number of parity blocks in the stripe
  const std::size_t nParity;
  //! the encoding matrix, required to compute any decode matrix
  std::vector<unsigned char> encode_matrix;
  //! a cache of previously used coding tables
  std::unordered_map<std::string, CodingTable> cache;
  //! concurrency control
  std::mutex mutex;
};

}

#endif	/* ERASURECODING_HH */


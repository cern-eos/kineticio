//------------------------------------------------------------------------------
//! @file ErasureCoding.hh
//! @author Paul Hermann Lensing
//! @brief Class for computing parities and recovering data
//------------------------------------------------------------------------------
#ifndef ERASURECODING_HH
#define  ERASURECODING_HH

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <mutex>
#include "LRUCache.hh"

namespace kio {

class ErasureCoding {
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
  //! Constructor.
  //! Stripe parameters (number of data and parity blocks) are constant per
  //! ErasureEncoding object.
  //!
  //! @param nData number of data blocks in stripes to be encoded by this object
  //! @param nParity number of parity blocks in stripes
  //--------------------------------------------------------------------------
  explicit ErasureCoding(std::size_t nData, std::size_t nParity, std::size_t nCodingTables);

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
  std::size_t nData;
  //! number of parity blocks in the stripe
  std::size_t nParity;
  //! the encoding matrix, required to compute any decode matrix
  std::vector<unsigned char> encode_matrix;
  //! a cache of previously used coding tables
  kio::LRUCache<std::string, CodingTable> cache;
  //! concurrency control
  std::mutex mutex;
};

}

#endif	/* ERASURECODING_HH */


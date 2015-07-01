#ifndef ERASEENCODING_HH
#define	ERASEENCODING_HH

#include <memory>
#include <vector>
#include <string>
#include <map>

class ErasureEncoding{
public:
  //--------------------------------------------------------------------------
  //! Compute all missing data and parity blocks in the the stripe. Stripe size
  //! has to equal nData+nParity. Blocks can be arbitrary size, but size has
  //! to be equal within a stripe. Function will throw on incorrect input.
  //!
  //! @param stripe nData+nParity blocks, missing (empty) blcoks will be computed
  //!        if possible.
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
  explicit ErasureEncoding(std::size_t nData, std::size_t nParity);
  ~ErasureEncoding();
  
  
private:
  //--------------------------------------------------------------------------
  //! Data structure to store all information required for a decode process with
  //! a known error pattern.
  //--------------------------------------------------------------------------
  struct CodingTable{
    //! The Coding Table.
    std::shared_ptr<unsigned char> table;
    //! array of nData size, containing stripe indices to input blocks
    std::shared_ptr<unsigned int> blockIndices;
    //! Number of Errors this Coding Table is constructed for (maximum nParity)
    int nErrors;
  };

  //--------------------------------------------------------------------------
  //! Constructs a string of the error pattern / signature. Each missing block
  //! in the stripe is counted as an error block, existing blocks are assumed
  //! to be correct (crc integrity checks of blocks should be done previously
  //! to attempting erasure decoding)
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
  std::unique_ptr<unsigned char> encode_matrix;
  //! a cache of previously used coding tables
  std::map<std::string, CodingTable> cache;
};

#endif	/* ERASEENCODING_HH */


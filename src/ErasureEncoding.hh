#ifndef ERASEENCODING_HH
#define	ERASEENCODING_HH

#include <memory>
#include <vector>
#include <string>
#include <map>

class ErasureEncoding{
public:
  ErasureEncoding(std::size_t nData, std::size_t nParity);
  ~ErasureEncoding();

  //--------------------------------------------------------------------------
  //! Compute all missing data and parity chunks in the the stripe. Stripe size
  //! has to equal nData+nParity. Chunks can be arbitrary size, but size has
  //! to be equal within a stripe. Function will throw on incorrect input. 
  //!
  //! @param stripe nData+nParity chunks, missing (empty) chunks will be computed
  //!        if possible. 
  //--------------------------------------------------------------------------
  void compute(std::vector<std::shared_ptr<const std::string> >& stripe);

private:
  struct CodingTable{
    //! The Coding Table.
    std::shared_ptr<unsigned char> table;
    //! array of nData size, containing stripe indices to input chunks
    std::shared_ptr<unsigned int> chunkIndices;
    //! Number of Errors this Coding Table is constructed for (maximum nParity)
    int nErrors;
  };

  /* Return a string of stripe size describing the error pattern. */
  std::string getErrorPattern(
      std::vector<std::shared_ptr<const std::string> >& stripe
  );

  CodingTable& getCodingTable(
      const std::string& pattern
  );

private:
  std::size_t nData;
  std::size_t nParity;
  std::size_t matrixSize;
  std::size_t tableSize;
  std::unique_ptr<unsigned char> encode_matrix;
  std::map<std::string, CodingTable> cache;
};

#endif	/* ERASEENCODING_HH */


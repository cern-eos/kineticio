#include "ErasureCoding.hh"
#include "Utility.hh"
#include <isa-l.h>

using std::string;
using std::make_shared;
using std::shared_ptr;
using namespace kio;


/* This function is (almost) completely ripped from the erasure_code_test.cc file
   distributed with the isa-l library. */
static int gf_gen_decode_matrix(
    unsigned char *encode_matrix, // in: encode matrix
    unsigned char *decode_matrix, // in: buffer, out: generated decode matrix
    unsigned int *decode_index,  // out: order of healthy blocks used for decoding [data#1, data#3, ..., parity#1... ]
    unsigned char *src_err_list,  // in: array of #nerrs size [index error #1, index error #2, ... ]
    unsigned char *src_in_err,    // in: array of #data size > [1,0,0,0,1,0...] -> 0 == no error, 1 == error
    int nerrs,                    // #total errors
    int nsrcerrs,                 // #data errors
    int k,                        // #data
    int m                         // #data+parity
)
{
  int i, j, p;
  int r;
  unsigned char *invert_matrix, *backup, *b, s;
  int incr = 0;

  std::vector<unsigned char> bv(m * k);
  b = bv.data();
  std::vector<unsigned char> backupv(m * k);
  backup = backupv.data();
  std::vector<unsigned char> invert_matrixv(m * k);
  invert_matrix = invert_matrixv.data();

  // Construct matrix b by removing error rows
  for (i = 0, r = 0; i < k; i++, r++) {
    while (src_in_err[r])
      r++;
    for (j = 0; j < k; j++) {
      b[k * i + j] = encode_matrix[k * r + j];
      backup[k * i + j] = encode_matrix[k * r + j];
    }
    decode_index[i] = r;
  }
  incr = 0;
  while (gf_invert_matrix(b, invert_matrix, k) < 0) {
    if (nerrs == (m - k)) {
      return -1;
    }
    incr++;
    memcpy(b, backup, m * k);
    for (i = nsrcerrs; i < nerrs - nsrcerrs; i++) {
      if (src_err_list[i] == (decode_index[k - 1] + incr)) {
        // skip the erased parity line
        incr++;
        continue;
      }
    }
    if (decode_index[k - 1] + incr >= m) {
      return -1;
    }
    decode_index[k - 1] += incr;
    for (j = 0; j < k; j++)
      b[k * (k - 1) + j] = encode_matrix[k * decode_index[k - 1] + j];

  };

  for (i = 0; i < nsrcerrs; i++) {
    for (j = 0; j < k; j++) {
      decode_matrix[k * i + j] = invert_matrix[k * src_err_list[i] + j];
    }
  }
  /* src_err_list from encode_matrix * invert of b for parity decoding */
  for (p = nsrcerrs; p < nerrs; p++) {
    for (i = 0; i < k; i++) {
      s = 0;
      for (j = 0; j < k; j++)
        s ^= gf_mul(invert_matrix[j * k + i],
                    encode_matrix[k * src_err_list[p] + j]);

      decode_matrix[k * p + i] = s;
    }
  }
  return 0;
}

ErasureCoding::ErasureCoding(std::size_t data, std::size_t parity, std::size_t codingTables) :
    nData(data), nParity(parity),
    encode_matrix((nData + nParity) * nData), cache(codingTables)
{
  // k = data
  // m = data + parity
  gf_gen_cauchy1_matrix(encode_matrix.data(), nData + nParity, nData);
}

std::string ErasureCoding::getErrorPattern(
    const std::vector<std::shared_ptr<const std::string> > &stripe
) const
{
  using utility::Convert;

  if (stripe.size() != nData + nParity)
    throw std::invalid_argument( Convert::toString(
        "ErasureCoding: Illegal stripe size. Expected ", nData + nParity, ", observed ", stripe.size()
    ));

  std::string pattern(nData + nParity, '0');
  int blockSize = 0;
  int nErrs = 0;

  for (int i = 0; i < stripe.size(); i++) {
    if (!stripe[i] || stripe[i]->empty()) {
      pattern[i] = 1;
      nErrs++;
    }
    else {
      pattern[i] = 0;
      if (!blockSize)
        blockSize = stripe[i]->size();
      if (blockSize != stripe[i]->size())
        throw std::invalid_argument( Convert::toString(
            "ErasureCoding: Non-static block sizes, observed one block with a size of ", blockSize, " bytes "
            "and another with a size of ", stripe[i]->size(), " bytes."
        ));
    }
  }
  if (nErrs > nParity)
    throw std::invalid_argument( Convert::toString(
        "ErasureCoding: More errors than parity blocks. ", nErrs, " errors, ", nParity, " parities."
    ));

  return pattern;
}

ErasureCoding::CodingTable& ErasureCoding::getCodingTable(
    const std::string &pattern
)
{
  std::lock_guard<std::mutex> lock(mutex);
  /* Check if decode matrix is already cached. */
  try {
    return cache.get(pattern);
  }catch(std::out_of_range e){

    /* Expand pattern */
    int nerrs = 0, nsrcerrs = 0;
    unsigned char err_indx_list[nParity];
    for (int i = 0; i < pattern.size(); i++) {
      if (pattern[i]) {
        err_indx_list[nerrs++] = i;
        if (i < nData) nsrcerrs++;
      }
    }

    /* Allocate Decode Object. */
    CodingTable dd;
    dd.nErrors = nerrs;
    dd.blockIndices.resize(nData);
    dd.table.resize(nData * nParity * 32);

    /* Compute decode matrix. */
    std::vector<unsigned char> decode_matrix((nData + nParity) * nData);

    if (gf_gen_decode_matrix(
        encode_matrix.data(),
        decode_matrix.data(),
        dd.blockIndices.data(),
        err_indx_list,
        (unsigned char *) pattern.c_str(),
        nerrs,
        nsrcerrs,
        static_cast<int>(nData),
        static_cast<int>(nParity + nData))
        )
      throw std::runtime_error("ErasureCoding: Failed computing decode matrix");

    /* Compute Tables. */
    ec_init_tables(nData, nerrs, decode_matrix.data(), dd.table.data());
    cache.add(pattern, dd);
  }
  return cache.get(pattern);
}

void ErasureCoding::compute(std::vector<std::shared_ptr<const std::string> > &stripe)
{
  if (!nParity)
    return;

  std::string pattern = getErrorPattern(stripe);
  auto &dd = getCodingTable(pattern);

  unsigned char *inbuf[nData];
  for (int i = 0; i < nData; i++) {
    inbuf[i] = (unsigned char *) stripe[dd.blockIndices[i]]->c_str();
  }

  auto blockSize = stripe[dd.blockIndices[0]]->size();
  std::vector<unsigned char> memory(dd.nErrors * blockSize);

  unsigned char *outbuf[dd.nErrors];
  for (int i = 0; i < dd.nErrors; i++) {
    outbuf[i] = &memory[i * blockSize];
  }

  ec_encode_data(
      blockSize,      // Length of each block of data (vector) of source or dest data.
      nData,          // The number of vector sources in the generator matrix for coding.
      dd.nErrors,     // The number of output vectors to concurrently encode/decode.
      dd.table.data(), // Pointer to array of input tables
      inbuf,          // Array of pointers to source input buffers
      outbuf          // Array of pointers to coded output buffers
  );

  int e = 0;
  for (int i = 0; i < nData + nParity; i++) {
    if (pattern[i]) {
      //printf("Repairing error %d (stripe index %d)\n",e,i);
      stripe[i] = make_shared<const string>(
          (const char *) outbuf[e], blockSize
      );
      e++;
    }
  }
}
//------------------------------------------------------------------------------
//! @file ClusterChunk.hh
//! @author Paul Hermann Lensing
//! @brief An fst-wide cache for cluster chunks. 
//------------------------------------------------------------------------------
#ifndef CLUSTERCHUNKCACHE_HH
#define  CLUSTERCHUNKCACHE_HH

/*----------------------------------------------------------------------------*/
#include "SequencePatternRecognition.hh"
#include "ClusterChunk.hh"
#include <unordered_map>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <memory>
#include <set>
/* <cstdatomic> is part of gcc 4.4.x experimental C++0x support... <atomic> is
 * what actually made it into the standard.*/
#if __GNUC__ == 4 && (__GNUC_MINOR__ == 4)
    #include <cstdatomic>
#else

#include <atomic>

#endif
/*----------------------------------------------------------------------------*/

namespace kio {

//! forward declare FileIo since FileIo includes the cache.
class FileIo;

//----------------------------------------------------------------------------
//! LRU cache for ClusterChunks. Threadsafe. Will create chunks
//! that are not in cache automatically during get()
//----------------------------------------------------------------------------
class ClusterChunkCache
{

public:
  enum class RequestMode { READAHEAD, STANDARD };

  //--------------------------------------------------------------------------
  //! Return the cluster chunk associated with the supplied owner and chunk
  //! number. Throws on error. ALSO throws, if a background flush has failed
  //! for the owner.
  //!
  //! @param owner a pointer to the kio::FileIo object the chunk belongs to
  //! @param chunk_number specifies which chunk of the file is requested,
  //! @param mode argument to pass to a chunk if it has to be created
  //! @return the chunk on success, throws on error
  //--------------------------------------------------------------------------
  std::shared_ptr<kio::ClusterChunk> get(
      kio::FileIo* owner,
      int chunknumber,
      ClusterChunk::Mode cm,
      RequestMode rm = RequestMode::STANDARD
  );

  //--------------------------------------------------------------------------
  //! Flushes all dirty chunks associated with the owner.
  //!
  //! @param owner a pointer to the kio::FileIo object the chunks belong to
  //--------------------------------------------------------------------------
  void flush(kio::FileIo* owner);

  //--------------------------------------------------------------------------
  //! Drops all chunks associated with the owner from the cache, dirty chunks
  //! are not flushed.
  //!
  //! @param owner a pointer to the kio::FileIo object the chunks belong to
  //--------------------------------------------------------------------------
  void drop(kio::FileIo* owner);

  //--------------------------------------------------------------------------
  //! Flushes the supplied chunk. If the number of current background threads
  //! does not exceed thread_capacity, the chunk will be flushed in the
  //! background. Otherwise, it will flush in the calling thread to prevent
  //! unlimited thread creation.
  //!
  //! @param owner a pointer to the kio::FileIo object the chunk belongs to
  //! @param chunk the chunk to flush
  //--------------------------------------------------------------------------
  void flush(kio::FileIo* owner, std::shared_ptr<kio::ClusterChunk> chunk);

  //--------------------------------------------------------------------------
  //! Cache is shared among all FileIo objects.
  //--------------------------------------------------------------------------
  static ClusterChunkCache& getInstance(){
    static ClusterChunkCache cc(200, 20);
    return cc;
  }


private:
  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param item_capacity maximum number of items in cache
  //! @param thread_capacity maximum number of threads to spawn for background
  //!        IO
  //--------------------------------------------------------------------------
  explicit ClusterChunkCache(size_t item_capacity, size_t thread_capacity);

  //--------------------------------------------------------------------------
  //! Destructor.
  //--------------------------------------------------------------------------
  ~ClusterChunkCache();

  //--------------------------------------------------------------------------
  //! Copy constructing makes no sense
  //--------------------------------------------------------------------------
  ClusterChunkCache(ClusterChunkCache&) = delete;

  //--------------------------------------------------------------------------
  //! Assignment make no sense
  //--------------------------------------------------------------------------
  void operator=(ClusterChunkCache&) = delete;

  //--------------------------------------------------------------------------
  //! Attempt to read the requested chunk number for the owner in a background
  //! thread. If this is not possible due to the number of active background
  //! threads already reaching thread_capacity just skip the readahead request.
  //! Errors during readahead will be swallowed, error handling can safely occur
  //! during the following regular read of the chunk.
  //!
  //! @param owner a pointer to the kio::FileIo object the chunk belongs to
  //! @param chunknumber the chunk number to read in
  //--------------------------------------------------------------------------
  void readahead(kio::FileIo* owner, int chunknumber);

  //--------------------------------------------------------------------------
  //! Readahead functionality that can be safely called in a detached
  //! std::thread
  //!
  //! @param owner a pointer to the kio::FileIo object the chunk belongs to
  //! @param chunk the chunk to read from the backend
  //--------------------------------------------------------------------------
  void threadsafe_readahead(std::shared_ptr<kio::ClusterChunk> chunk);

  //--------------------------------------------------------------------------
  //! Flush functionality that can be safely called in a detached std::thread
  //!
  //! @param owner a pointer to the kio::FileIo object the chunk belongs to
  //! @param chunk the chunk to flush to the backend
  //--------------------------------------------------------------------------
  void threadsafe_flush(kio::FileIo* owner, std::shared_ptr<kio::ClusterChunk> chunk);

private:
  //! maximum number of items allowed in the cache
  const size_t item_capacity;
  //! maximum number of threads used for I/O
  const size_t thread_capacity;
  //! the current number of threads used for I/O
  std::atomic<int> numthreads;

  //! The cache item structure
  struct CacheItem
  {
    //! the chunk number
    const int id;
    //! the chunk owner
    const kio::FileIo* owner;
    //! the actual chunk
    std::shared_ptr<kio::ClusterChunk> chunk;
  };

  //! A linked list of CacheItems stored in LRU order
  std::list<CacheItem> cache;
  typedef std::list<CacheItem>::iterator cache_iterator;
  typedef std::unordered_map<int, cache_iterator> chunk_map;

  //! the primary lookup table contains a chunk lookup table for every owner
  std::unordered_map<const FileIo*, chunk_map> lookup;

  //! Exceptions occurring during background execution are stored and thrown at
  //! the next get request of the owner.
  std::unordered_map<const kio::FileIo*, std::exception> exceptions;

  //! Track per FileIo access patterns and attempt to pre-fetch intelligently
  std::unordered_map<const kio::FileIo*, SequencePatternRecognition> prefetch;

  //! Thread safety when accessing exception map
  std::mutex exception_mutex;

  //! Thread safety when accessing pre-fetch map
  std::mutex readahead_mutex;

  //! Thread safety when accessing cache structures (lookup table and lru list)
  std::mutex cache_mutex;
};



}

#endif	/* CLUSTERCHUNKCACHE_HH */


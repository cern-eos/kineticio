//------------------------------------------------------------------------------
//! @file ClusterChunk.hh
//! @author Paul Hermann Lensing
//! @brief An fst-wide cache for cluster chunks. 
//------------------------------------------------------------------------------
#ifndef CLUSTERCHUNKCACHE_HH
#define  CLUSTERCHUNKCACHE_HH

/*----------------------------------------------------------------------------*/
#include "SequencePatternRecognition.hh"
#include "BackgroundOperationHandler.hh"
#include "ClusterChunk.hh"
#include <unordered_map>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <memory>
#include <set>
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
  void async_flush(kio::FileIo* owner, std::shared_ptr<kio::ClusterChunk> chunk);

  //--------------------------------------------------------------------------
  //! Return a value between [0...1] showing the current cache pressure. This
  //! can be used to throttle workload to guard against cache thrashing.
  //!
  //! @return value between 0 and 1 signifying cache pressure.
  //--------------------------------------------------------------------------
  double pressure();

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param preferred_size size in bytes the cache should ideally not exceed
  //! @param capacity absolute maximum size of the cache in bytes
  //! @param thread_capacity maximum number of threads to spawn for background
  //!        IO
  //--------------------------------------------------------------------------
  explicit ClusterChunkCache(size_t preferred_size, size_t capacity, size_t thread_capacity);

  //--------------------------------------------------------------------------
  //! No copy constructor.
  //--------------------------------------------------------------------------
  ClusterChunkCache(ClusterChunkCache&) = delete;

  //--------------------------------------------------------------------------
  //! No copy assignment.
  //--------------------------------------------------------------------------
  void operator=(ClusterChunkCache&) = delete;

private:
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
  //! Flush functionality
  //!
  //! @param owner a pointer to the kio::FileIo object the chunk belongs to
  //! @param chunk the chunk to flush to the backend
  //--------------------------------------------------------------------------
  void do_flush(kio::FileIo *owner, std::shared_ptr<kio::ClusterChunk> chunk);

private:
  //! preferred size of the cache (soft cap)
  const std::size_t target_size;

  //! maximum size of the cache (hard cap)
  const std::size_t capacity;

  //! current size of the cache
  std::atomic<std::size_t> current_size;

  //! handle background readahead and flush requests
  BackgroundOperationHandler bg;

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
  typedef std::list<CacheItem>::iterator cache_iterator;
  typedef std::unordered_map<int, cache_iterator> chunk_map;

  //! A linked list of CacheItems stored in LRU order
  std::list<CacheItem> cache;

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


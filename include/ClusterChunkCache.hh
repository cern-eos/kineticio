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
class ClusterChunkCache {

public:
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
  enum class RequestMode {
    READAHEAD, STANDARD
  };

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
  //! Drop the owner from the cache, optionally also drop associated chunks
  //! (dirty chunks will not be flushed in this case).
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
  //! The configuration of an existing ClusterChunkCache object can be changed
  //! during runtime.
  //!
  //! @param preferred_size size in bytes the cache should ideally not exceed
  //! @param capacity absolute maximum size of the cache in bytes
  //! @param bg_threads number of threads to spawn for background IO
  //! @param bg_queue_depth maximum number of functions queued for background
  //!   execution
  //--------------------------------------------------------------------------
  void changeConfiguration(size_t preferred_size, size_t capacity, size_t bg_threads, size_t bg_queue_depth);

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param preferred_size size in bytes the cache should ideally not exceed
  //! @param capacity absolute maximum size of the cache in bytes
  //! @param bg_threads number of threads to spawn for background IO
  //! @param bg_queue_depth maximum number of functions queued for background
  //!   execution
  //--------------------------------------------------------------------------
  explicit ClusterChunkCache(size_t preferred_size, size_t capacity, size_t bg_threads, size_t bg_queue_depth);

  //--------------------------------------------------------------------------
  //! No copy constructor.
  //--------------------------------------------------------------------------
  ClusterChunkCache(ClusterChunkCache&) = delete;

  //--------------------------------------------------------------------------
  //! No copy assignment.
  //--------------------------------------------------------------------------
  void operator=(ClusterChunkCache&) = delete;

private:
  //! preferred size of the cache (soft cap), atomic so it may be changed during runtime
  std::atomic<std::size_t> target_size;

  //! maximum size of the cache (hard cap), atomic so it may be changed during runtime
  std::atomic<std::size_t> capacity;

  //! current size of the cache
  std::atomic<std::size_t> current_size;

  //! handle background readahead and flush requests
  BackgroundOperationHandler bg;

  struct CacheItem {
    std::set<kio::FileIo*> owners;
    std::shared_ptr<kio::ClusterChunk> chunk;
  };

  //! A linked list of cluster chunks stored in LRU order
  std::list<CacheItem> cache;

  //! the lookup table
  typedef std::list<CacheItem>::iterator cache_iterator;
  std::unordered_map<std::string, cache_iterator> lookup;

  //! comparison operator so we can create std::set<cache_iterator>
  struct cache_iterator_compare {
    bool operator()(const cache_iterator& lhs, const cache_iterator& rhs) const
    {
      return lhs->chunk < rhs->chunk;
    }
  };

  //! keep set of cache items associated with each owner (for drop & flush commands)
  std::unordered_map<const kio::FileIo*, std::set<cache_iterator, cache_iterator_compare>> owner_tables;

  //! Exceptions occurring during background execution are stored and thrown at
  //! the next get request of the owner.
  std::unordered_map<const kio::FileIo*, std::exception> exceptions;

  //! Track per FileIo access patterns and attempt to pre-fetch intelligently
  std::unordered_map<const kio::FileIo*, SequencePatternRecognition> prefetch;

  //! Ratelimit attempts to shrink the current cache size by removing clean items from the tail
  std::chrono::system_clock::time_point cache_cleanup_timestamp;

  //! Thread safety when accessing cache_cleanup_timestamp
  std::mutex cache_cleanup_mutex;

  //! Thread safety when accessing exception map
  std::mutex exception_mutex;

  //! Thread safety when accessing pre-fetch map
  std::mutex readahead_mutex;

  //! Thread safety when accessing cache structures (lookup table and lru list)
  std::mutex cache_mutex;

private:

  //--------------------------------------------------------------------------
  //! Return a value between [0...1] showing the current cache pressure. This
  //! can be used to throttle workload to guard against cache thrashing.
  //!
  //! @return value between 0 and 1 signifying cache pressure.
  //--------------------------------------------------------------------------
  double cache_pressure();

  //--------------------------------------------------------------------------
  //! Block in case the cache is under pressure.
  //--------------------------------------------------------------------------
  void throttle();

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
  void do_flush(kio::FileIo* owner, std::shared_ptr<kio::ClusterChunk> chunk);

  //--------------------------------------------------------------------------
  //! Remove an item from the cache as well as the lookup table and from
  //! associated owners.
  //!
  //! @param it an iterator to the element to be removed
  //! @return iterator to following element
  //!--------------------------------------------------------------------------
  cache_iterator remove_item(const cache_iterator& it);
};


}

#endif	/* CLUSTERCHUNKCACHE_HH */


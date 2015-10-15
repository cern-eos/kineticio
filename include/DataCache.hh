//------------------------------------------------------------------------------
//! @file DataCache.hh
//! @author Paul Hermann Lensing
//! @brief A library wide cache for data blocks. 
//------------------------------------------------------------------------------
#ifndef KINETICIO_DATACACHE_HH
#define KINETICIO_DATACACHE_HH

/*----------------------------------------------------------------------------*/
#include "PrefetchOracle.hh"
#include "BackgroundOperationHandler.hh"
#include "DataBlock.hh"
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
//! LRU cache for Data. Threadsafe. Will create blocks
//! that are not in cache automatically during get()
//----------------------------------------------------------------------------
class DataCache {

public:
  //--------------------------------------------------------------------------
  //! Return the data block associated with the supplied owner and block
  //! number. Throws on error. ALSO throws, if a background flush has failed
  //! for the owner.
  //!
  //! @param owner a pointer to the kio::FileIo object the block belongs to
  //! @param blocknumber specifies which block of the file is requested,
  //! @param mode argument to pass to a block if it has to be created
  //! @return the block on success, throws on error
  //--------------------------------------------------------------------------
  enum class RequestMode {
    READAHEAD, STANDARD
  };

  std::shared_ptr<kio::DataBlock> get(
      kio::FileIo* owner,
      int blocknumber,
      DataBlock::Mode cm,
      RequestMode rm = RequestMode::STANDARD
  );

  //--------------------------------------------------------------------------
  //! Flushes all dirty data associated with the owner.
  //!
  //! @param owner a pointer to the kio::FileIo object the data belongs to
  //--------------------------------------------------------------------------
  void flush(kio::FileIo* owner);

  //--------------------------------------------------------------------------
  //! Drop the owner from the cache, optionally also drop associated blocks
  //! (dirty blocks will not be flushed in this case).
  //!
  //! @param force, if set to true will throw away all blocks associated with the owner
  //! @param owner a pointer to the kio::FileIo object the blocks belong to
  //--------------------------------------------------------------------------
  void drop(kio::FileIo* owner, bool force=false);

  //--------------------------------------------------------------------------
  //! Flushes the supplied data. If the number of current background threads
  //! does not exceed thread_capacity, the data will be flushed in the
  //! background. Otherwise, it will flush in the calling thread to prevent
  //! unlimited thread creation.
  //!
  //! @param owner a pointer to the kio::FileIo object the data belongs to
  //! @param data the data to flush
  //--------------------------------------------------------------------------
  void async_flush(kio::FileIo* owner, std::shared_ptr<kio::DataBlock> data);

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
  void changeConfiguration(size_t preferred_size, size_t capacity, size_t bg_threads, size_t bg_queue_depth, size_t readahead_size);

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param preferred_size size in bytes the cache should ideally not exceed
  //! @param capacity absolute maximum size of the cache in bytes
  //! @param bg_threads number of threads to spawn for background IO
  //! @param bg_queue_depth maximum number of functions queued for background
  //!   execution
  //--------------------------------------------------------------------------
  explicit DataCache(size_t preferred_size, size_t capacity, size_t bg_threads, size_t bg_queue_depth, size_t readahead_window_size);

  //--------------------------------------------------------------------------
  //! No copy constructor.
  //--------------------------------------------------------------------------
  DataCache(DataCache&) = delete;

  //--------------------------------------------------------------------------
  //! No copy assignment.
  //--------------------------------------------------------------------------
  void operator=(DataCache&) = delete;

private:
  //! preferred size of the cache (soft cap), atomic so it may be changed during runtime
  std::atomic<size_t> target_size;

  //! maximum size of the cache (hard cap), atomic so it may be changed during runtime
  std::atomic<size_t> capacity;

  //! current size of the cache
  std::atomic<size_t> current_size;
  
  //! percentage of dirty data keys in the cache tail
  std::atomic<int> write_pressure; 

  //! the maximum number of blocks to prefetch 
  int readahead_window_size;
  
  //! handle background readahead and flush requests
  BackgroundOperationHandler bg;

  struct CacheItem {
    std::set<kio::FileIo*> owners;
    std::shared_ptr<kio::DataBlock> data;
    std::chrono::system_clock::time_point last_access;
  };

  //! A linked list of data blocks stored in LRU order
  std::list<CacheItem> cache;

  //! the lookup table
  typedef std::list<CacheItem>::iterator cache_iterator;
  std::unordered_map<std::string, cache_iterator> lookup;

  //! comparison operator so we can create std::set<cache_iterator>
  struct cache_iterator_compare {
    bool operator()(const cache_iterator& lhs, const cache_iterator& rhs) const
    {
      return lhs->data < rhs->data;
    }
  };

  //! keep set of cache items associated with each owner (for drop & flush commands)
  std::unordered_map<const kio::FileIo*, std::set<cache_iterator, cache_iterator_compare>> owner_tables;

  //! Exceptions occurring during background execution are stored and thrown at
  //! the next get request of the owner.
  std::unordered_map<const kio::FileIo*, std::exception> exceptions;

  //! Track per FileIo access patterns and attempt to pre-fetch intelligently
  std::unordered_map<const kio::FileIo*, PrefetchOracle> prefetch;

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
  //! Block in case the cache is under pressure.
  //--------------------------------------------------------------------------
  void throttle();

  //--------------------------------------------------------------------------
  //! Attempt to read the requested block number for the owner in a background
  //! thread. If this is not possible due to the number of active background
  //! threads already reaching thread_capacity just skip the readahead request.
  //! Errors during readahead will be swallowed, error handling can safely occur
  //! during the following regular read of the data.
  //!
  //! @param owner a pointer to the kio::FileIo object the data belongs to
  //! @param blocknumber the data block number to read in
  //--------------------------------------------------------------------------
  void readahead(kio::FileIo* owner, int blocknumber);

  //--------------------------------------------------------------------------
  //! Flush functionality
  //!
  //! @param owner a pointer to the kio::FileIo object the data belongs to
  //! @param data the data to flush to the backend
  //--------------------------------------------------------------------------
  void do_flush(kio::FileIo* owner, std::shared_ptr<kio::DataBlock> data);

  //--------------------------------------------------------------------------
  //! Remove an item from the cache as well as the lookup table and from
  //! associated owners.
  //!
  //! @param it an iterator to the element to be removed
  //! @return iterator to following element
  //!--------------------------------------------------------------------------
  cache_iterator remove_item(const cache_iterator& it);
  
  void cache_to_target_size(); 
};


}

#endif	/* CLUSTERCHUNKCACHE_HH */


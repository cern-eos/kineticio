#ifndef CLUSTERCHUNKCACHE_HH
#define	CLUSTERCHUNKCACHE_HH

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


namespace kio{

// forward declare FileIo since FileIo includes the cache.
class FileIo;

//----------------------------------------------------------------------------
//! LRU cache for ClusterChunks. Threadsafe. Will obtain chunks
//! that are not in cache automatically from the backend during get()
//----------------------------------------------------------------------------
class ChunkCache {

public:
  //--------------------------------------------------------------------------
  //!
  //! @param chunk_number specifies which chunk in the file is requested,
  //! @param chunk points to chunk on success, otherwise not changed
  //! @return the chunk on success, throws on error
  //--------------------------------------------------------------------------
  std::shared_ptr<kio::ClusterChunk> get(kio::FileIo* owner, int chunknumber);

  void flush(kio::FileIo* owner);
  void drop(kio::FileIo* owner);

  //! done in background
  void flush(kio::FileIo* owner, std::shared_ptr<kio::ClusterChunk> chunk);
  void readahead(kio::FileIo* owner, int chunknumber);

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param cache_capacity maximum number of items in chache
  //--------------------------------------------------------------------------
  explicit ChunkCache(size_t capacity);

  //--------------------------------------------------------------------------
  //! Destructor.
  //--------------------------------------------------------------------------
  ~ChunkCache();

private:
  void threadsafe_readahead(kio::FileIo* owner, std::shared_ptr<kio::ClusterChunk> chunk);
  void threadsafe_flush(kio::FileIo* owner, std::shared_ptr<kio::ClusterChunk> chunk);

private:
  //! maximum number of items allowed in the cache
  const size_t item_capacity;
  const size_t thread_capacity;

  std::atomic<int> numthreads;

  struct CacheItem{
    const int id;
    const kio::FileIo* owner;
    std::shared_ptr<kio::ClusterChunk> chunk;
  };
  std::list<CacheItem> cache;

  typedef std::list<CacheItem>::iterator cache_iterator;
  typedef std::unordered_map<int, cache_iterator> chunk_map;

  std::unordered_map<const FileIo*, chunk_map> lookup;

  /* Exceptions occurring during background execution are thrown at the next
     request by owner. */
  std::unordered_map<const kio::FileIo*, std::exception>  exceptions;

  std::mutex exception_mutex;

  /* We could lock much finer grained on a per-owner basis for most operations */
  std::mutex cache_mutex;


};

//! Static ClusterMap for all KineticFileIo objects
static ChunkCache & ccache()
{
  static ChunkCache cc(100);
  return cc;
}

}

#endif	/* CLUSTERCHUNKCACHE_HH */


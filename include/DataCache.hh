//------------------------------------------------------------------------------
//! @file DataCache.hh
//! @author Paul Hermann Lensing
//! @brief A library wide cache for data blocks. 
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
  std::shared_ptr<kio::DataBlock> get(
      kio::FileIo* owner,
      int blocknumber,
      DataBlock::Mode cm
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
  //! Return current cache utilization as a double value between 0 and 1.
  //!
  //! @return cache utilization
  //--------------------------------------------------------------------------
  double utilization();

  //--------------------------------------------------------------------------
  //! The configuration of an existing ClusterChunkCache object can be changed
  //! during runtime.
  //!
  //! @param capacity absolute maximum size of the cache in bytes
  //--------------------------------------------------------------------------
  void changeConfiguration(size_t capacity);

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param capacity absolute maximum size of the cache in bytes
  //--------------------------------------------------------------------------
  explicit DataCache(size_t capacity);

  //--------------------------------------------------------------------------
  //! No copy constructor.
  //--------------------------------------------------------------------------
  DataCache(DataCache&) = delete;

  //--------------------------------------------------------------------------
  //! No copy assignment.
  //--------------------------------------------------------------------------
  void operator=(DataCache&) = delete;

private:
  //! maximum size of the cache (hard cap), atomic so it may be changed during runtime
  std::atomic<size_t> capacity;

  //! current size of the cache
  std::atomic<size_t> current_size;

  //! current size of the unused items list
  size_t unused_size;
  
  struct CacheItem {
    std::set<kio::FileIo*> owners;
    std::shared_ptr<kio::DataBlock> data;
    std::chrono::system_clock::time_point last_access;
  };

  //! A linked list of data blocks stored in LRU order
  std::list<CacheItem> cache;

  // List of items that are no longer used but kept around for future re-use to avoid memory allocation. 
  std::list<CacheItem> unused_items; 
  
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

  //! Thread safety when accessing cache structures (lookup table and lru list)
  std::mutex cache_mutex;

private:
  //--------------------------------------------------------------------------
  //! Remove an item from the cache as well as the lookup table and from
  //! associated owners.
  //!
  //! @param it an iterator to the element to be removed
  //! @return iterator to following element
  //!--------------------------------------------------------------------------
  cache_iterator remove_item(const cache_iterator& it);
  
  //--------------------------------------------------------------------------
  //! Attempt to shrink the cache by discarding unused items from the 
  //! cache tail. 
  //--------------------------------------------------------------------------
  void try_shrink(); 
};


}

#endif	/* CLUSTERCHUNKCACHE_HH */


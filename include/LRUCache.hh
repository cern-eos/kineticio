//------------------------------------------------------------------------------
//! @file LRUCache.hh
//! @author Paul Hermann Lensing
//! @brief A (very) simple general purpose LRU cache.
//------------------------------------------------------------------------------
#ifndef KINETICIO_LRUCACHE_HH
#define KINETICIO_LRUCACHE_HH

/*----------------------------------------------------------------------------*/
#include <unordered_map>
#include <utility>
#include <list>
/*----------------------------------------------------------------------------*/

namespace kio {

//----------------------------------------------------------------------------
//! Minimalistic general purpose LRU cache.
//----------------------------------------------------------------------------
template<typename Key, typename Value> class LRUCache {
public:
  //----------------------------------------------------------------------------
  //! Adds a k-v pair to the front of the cache. If the key is already in the
  //! cache, the existing entry will simply become inaccessible.
  //!
  //! @param k the key
  //! @param v the value
  //----------------------------------------------------------------------------
  void add(const Key& k, const Value& v){
    cache.push_front(cache_item(k, v));
    lookup[k] = cache.begin();

    if (cache.size() > capacity)
      shrink();
  }

  //----------------------------------------------------------------------------
  //! Return the value associated with the supplied key while maintaining LRU
  //! order in the cache. If the requested key is not in the cache, a
  //! std::out_of_range exception will be thrown by std::map::at()
  //!
  //! @param k the key
  //! @return the value
  //----------------------------------------------------------------------------
  Value& get(const Key& k){
    cache.splice(cache.begin(), cache, lookup.at(k));
    return cache.front().second;
  }

  //----------------------------------------------------------------------------
  //! Set the supplied capacity.
  //!
  //! @param cap maximum number of elements in the cache
  //----------------------------------------------------------------------------
  void setCapacity(size_t cap){
    capacity = cap;
    while(cache.size() > capacity)
      shrink();
  }

  //----------------------------------------------------------------------------
  //! Get the cache capacity.
  //!
  //! @return maximum number of elements in the cache
  //----------------------------------------------------------------------------
  size_t getCapacity(){
    return capacity;
  }

  //----------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param capacity maximum number of elements in the cache
  //----------------------------------------------------------------------------
  explicit LRUCache(std::size_t capacity) : capacity(capacity) { }

private:
  //----------------------------------------------------------------------------
  //! Shrink the cache by one element, removing the last element in the LRU
  //! list.
  //----------------------------------------------------------------------------
  void shrink(){
    /* The last element in the cache might not be in the lookup table
     * any more if the same key has been added multiple times. */
    if (lookup[cache.back().first] == --cache.end())
      lookup.erase(cache.back().first);
    cache.pop_back();
  }

private:
  typedef std::pair<Key, Value> cache_item;
  typedef typename std::list<cache_item>::iterator cache_iterator;

  //! maximum number of items in the cache.
  size_t capacity;

  //! the cache
  std::list<cache_item> cache;

  //! the lookup table for items in the cache
  std::unordered_map<Key, cache_iterator> lookup;
};

}


#endif


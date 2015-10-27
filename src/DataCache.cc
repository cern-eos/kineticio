#include "DataCache.hh"
#include "FileIo.hh"
#include "Utility.hh"
#include "Logging.hh"
#include "KineticCluster.hh"
#include <thread>
#include <unistd.h>

using namespace kio;


DataCache::DataCache(size_t preferred_size, size_t capacity, size_t bg_threads,
                                     size_t bg_queue_depth, size_t readahead_size) :
    target_size(preferred_size), capacity(capacity), current_size(0), bg(bg_threads, bg_queue_depth),
    readahead_window_size(readahead_size)
{
  if (capacity < target_size) throw std::logic_error("cache target size may not exceed capacity");
  if (bg_threads < 0) throw std::logic_error("number of background threads cannot be negative.");
}

void DataCache::changeConfiguration(size_t preferred_size, size_t cap, size_t bg_threads,
                                            size_t bg_queue_depth, size_t readahead_size)
{
  {
    std::lock_guard<std::mutex> lock(readahead_mutex);  
    readahead_window_size = readahead_size;
  }
  target_size = preferred_size;
  capacity = cap;
  bg.changeConfiguration(bg_threads, bg_queue_depth);
}

void DataCache::drop(kio::FileIo* owner, bool force)
{
  /* If we encountered a exception in a background flush, we don't care
     about it if we are dropping the data anyways. */
  { std::lock_guard<std::mutex> lock(exception_mutex);
    if (exceptions.count(owner))
      exceptions.erase(owner);
  }
  { std::lock_guard<std::mutex> lock(readahead_mutex);
    if (prefetch.count(owner))
      prefetch.erase(owner);
  }

  std::lock_guard<std::mutex> lock(cache_mutex);
  if(owner_tables.count(owner)) {
    for (auto owit = owner_tables[owner].cbegin(); owit != owner_tables[owner].cend(); owit++) {
      cache_iterator it = *owit;
      it->owners.erase(owner);
      /* Because some clients apparently like re-opening files, we will no longer automatically remove orphaned 
       * data keys (unless force is set)... they will only be removed when cache pressure indicates.  */
      if(force)
        remove_item(it);
    }
  }
  owner_tables.erase(owner);
}

void DataCache::flush(kio::FileIo* owner)
{
  /* If we encountered a exception in a background flush, we don't care
     about it, if it is still an issue we will re-encounter it during the flush operation. */
  { std::lock_guard<std::mutex> lock(exception_mutex);
    if (exceptions.count(owner))
      exceptions.erase(owner);
  }

  /* build a vector of blocks, so we can flush without holding cache_mutex */
  std::vector<std::shared_ptr<kio::DataBlock> > blocks;
  { std::lock_guard<std::mutex> lock(cache_mutex);
    if (owner_tables.count(owner)) {
      for (auto item = owner_tables[owner].cbegin(); item != owner_tables[owner].cend(); item++) {
        cache_iterator it = *item;
        blocks.push_back(it->data);
      }
    }
  }

  for (auto it = blocks.begin(); it != blocks.end(); it++) {
    auto& block = *it;
    if (block->dirty()) {
      block->flush();
    }
  }
}

void DataCache::try_shrink()
{
    if(current_size < target_size){
      write_pressure = 0;
      return; 
    }

    using namespace std::chrono;
    auto expired = system_clock::now() - seconds(5);
    auto num_items = (current_size / cache.back().data->capacity()) * 0.1;
    auto count_items = 0; 
    auto count_dirty = 0; 
        
    kio_debug("Cache current size is ", current_size, " bytes, target size is ", target_size, " bytes. Shrinking cache.");
    for (auto it = --cache.end(); num_items > count_items && it != cache.begin(); it--, count_items++) {
      if(it->data->dirty()){
        count_dirty++;
        continue;
      }
      if (it->owners.empty() || it->last_access < expired)
        it = remove_item(it);
    }
    
    write_pressure = (count_dirty*100) / num_items;
}

void DataCache::throttle()
{
  using namespace std::chrono;
  static const milliseconds ratelimit(50);

  for(auto wait_pressure = 1; wait_pressure < write_pressure; wait_pressure++) 
  {
    kio_debug("Throttling...");
    {
      std::lock_guard<std::mutex> ratelimit_lock(cache_cleanup_mutex);
      if(duration_cast<milliseconds>(system_clock::now() - cache_cleanup_timestamp) > ratelimit){
        cache_cleanup_timestamp = system_clock::now();
        std::lock_guard<std::mutex> cachelock(cache_mutex);
        try_shrink();      
      }
    }
    /* Sleep 100 ms to give dirty data a chance to flush before retrying */
    usleep(1000 * 100);
  }
}

DataCache::cache_iterator DataCache::remove_item(const cache_iterator& it)
{
  kio_debug("Removing cache key ", it->data->getIdentity(), " from cache.");
  for(auto o = it->owners.cbegin(); o!= it->owners.cend(); o++)
    owner_tables[*o].erase(it);
   
  lookup.erase(it->data->getIdentity());
  current_size -= it->data->capacity();
  return cache.erase(it);
}

std::shared_ptr<kio::DataBlock> DataCache::get(kio::FileIo* owner, int blocknumber, DataBlock::Mode mode, bool prefetch)
{
  { std::lock_guard<std::mutex> exeptionlock(exception_mutex);
    if (exceptions.count(owner)) {
      std::exception e = exceptions[owner];
      exceptions.erase(owner);
      throw e;
    }
  }

  /* Throttle this request as indicated by cache pressure */
  throttle();
   
  /* We cannot use the block key directly for cache lookups, as reloading configuration will create 
     different cluster objects and we have to avoid FileIo objects being associated with multiple clusters */
  auto block_key = utility::constructBlockKey(owner->block_basename, blocknumber);
  std::string cache_key = owner->cluster->id() + *block_key;
  
  /* Register requested block with read-ahead logic if requested. */
  if (prefetch)
    readahead(owner, blocknumber);

  std::lock_guard<std::mutex> cachelock(cache_mutex);
  /* If the requested block is already cached, we can return it without IO. */
  if (lookup.count(cache_key)) {
    kio_debug("Serving data key ", *block_key, " for owner ", owner, " from cache.");
    
    /* Splicing the element into the front of the list will keep iterators valid. */
    cache.splice(cache.begin(), cache, lookup[cache_key]);

    /* set owner<->cache_item relationship. Since we have std::sets there's no need to test for existence */
    owner_tables[owner].insert(cache.begin());
    cache.front().owners.insert(owner);
    
    /* Update access timestamp */
    cache.front().last_access = std::chrono::system_clock::now();
    return cache.front().data;
  }
  
 
  /* Attempt to shrink cache size to target size by releasing non-dirty items only*/ 
  try_shrink();   

  /* If cache size would exceed capacity, we have to try flushing dirty blocks manually. */
  if (capacity < current_size + owner->cluster->limits().max_value_size) {
    kio_notice("Cache capacity reached.");
    auto& it = --cache.end();
    if (it->data->dirty()) {
      try {
        it->data->flush();
      }
      catch (const std::exception& e) {
        throw kio_exception(EIO, "Failed freeing cache space: ", e.what());
      }
    }
    remove_item(it);
  }

  cache.push_front(
    CacheItem{ std::set<kio:: FileIo*>{owner}, 
               std::make_shared<DataBlock>(owner->cluster, block_key, mode), 
               std::chrono::system_clock::now()
    }
  );
  current_size += cache.front().data->capacity();
  lookup[cache_key] = cache.begin();
  owner_tables[owner].insert(cache.begin());
  
  kio_debug("Added data key ", *block_key, " to the cache for owner ", owner);
  return cache.front().data;;
}

void DataCache::do_flush(kio::FileIo* owner, std::shared_ptr<kio::DataBlock> data)
{
  if (data->dirty()) {
    try {
      data->flush();
    }
    catch (const std::exception& e) {
      std::lock_guard<std::mutex> lock(exception_mutex);
      exceptions[owner] = e;
    }
  }
}

void DataCache::async_flush(kio::FileIo* owner, std::shared_ptr<DataBlock> data)
{
  bg.run(std::bind(&DataCache::do_flush, this, owner, data));
}

void do_readahead(std::shared_ptr<kio::DataBlock> data)
{
  /* if readahaed should throw, there's no need to remember as in do_flush...  we'll just re-encounter the exception
   * should the block actually be read from */
  char buf[1];
  data->read(buf, 0, 1);
}

void DataCache::readahead(kio::FileIo* owner, int blocknumber)
{
  PrefetchOracle* oracle; 
  { std::lock_guard<std::mutex> readaheadlock(readahead_mutex);
    if(!prefetch.count(owner))
      prefetch[owner] = PrefetchOracle(readahead_window_size);
    oracle = &prefetch[owner];
    oracle->add(blocknumber);
  }
  
  /* Only do readahead if cache isn't bursting already. */
  if (current_size < target_size + 0.5*(capacity-target_size)){
    auto prediction = oracle->predict(PrefetchOracle::PredictionType::CONTINUE);
    for (auto it = prediction.cbegin(); it != prediction.cend(); it++) {
      auto data = get(owner, *it, DataBlock::Mode::STANDARD, false);
      auto scheduled = bg.try_run(std::bind(do_readahead, data));
      if(scheduled)
        kio_debug("Readahead of data block with identity ", data->getIdentity(), " scheduled for owner ", owner);
    }
  }
}




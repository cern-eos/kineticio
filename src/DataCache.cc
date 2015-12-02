#include "DataCache.hh"
#include "FileIo.hh"
#include "Utility.hh"
#include "Logging.hh"
#include "KineticCluster.hh"
#include "KineticIoSingleton.hh"
#include <thread>
#include <unistd.h>

using namespace kio;


DataCache::DataCache(size_t capacity, size_t readahead_size) :
    capacity(capacity), current_size(0), unused_size(0),
    readahead_window_size(readahead_size)
{
}

void DataCache::changeConfiguration(size_t cap, size_t readahead_size)
{
  readahead_window_size = readahead_size;
  capacity = cap;
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

DataCache::cache_iterator DataCache::remove_item(const cache_iterator& it)
{
  for(auto o = it->owners.cbegin(); o!= it->owners.cend(); o++)
    owner_tables[*o].erase(it);
   
  lookup.erase(it->data->getIdentity());
  current_size -= it->data->capacity();

  /* We don't want to keep too many unused cache items around... */
  if(unused_size > 0.1 * capacity){
    kio_debug("Deleting cache key ", it->data->getIdentity(), " from cache.");
    return cache.erase(it);
  }
    
  kio_debug("Transferring cache key ", it->data->getIdentity(), " from cache to unused items pool.");  
  auto next_it = std::next(it);
  unused_size += it->data->capacity();
  unused_items.splice(unused_items.begin(), cache, it);
  return next_it; 
}

void DataCache::try_shrink()
{
  using namespace std::chrono;
  auto expired = system_clock::now() - seconds(5);
  auto num_items = (current_size / cache.back().data->capacity()) * 0.1;
  auto count_items = 0; 

  kio_debug("Cache current size is ", current_size, " bytes. Shrinking cache.");
  for (auto it = --cache.end(); num_items > count_items && it != cache.begin(); it--, count_items++) {
    if ((it->owners.empty() || it->last_access < expired) && !it->data->dirty() && it->data.unique())
      it = remove_item(it);
  }

  /* If cache size exceeds capacity, we have to force remove data keys. */
  if(capacity < current_size) {
    kio_notice("Cache capacity reached.");

    for (auto it = --cache.end(); capacity < current_size && it != cache.begin(); it--){
      if(it->data.unique() && !it->data->dirty())
        remove_item(it);
    }
    
    /* Second try.. can't find an ideal candidate, we will flush. */
    for (auto it = --cache.end(); capacity < current_size && it != cache.begin(); it--){
      if(it->data.unique()){
        if (it->data->dirty()) {
          try {
            it->data->flush();
          }
          catch (const std::exception& e) {
            kio_warning("Failed flushing cache item ", it->data->getIdentity(), "  Reason: ", e.what());
            continue;
          }
        }
        remove_item(it);
      }
    }  
  }
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
   
  /* We cannot use the block key directly for cache lookups, as reloading the configuration will create 
     different cluster objects and we have to avoid FileIo objects being associated with multiple clusters */
  auto data_key = utility::makeDataKey(owner->cluster->id(), owner->path, blocknumber);
  std::string cache_key = *data_key + owner->cluster->instanceId();
  
  /* Register requested block with read-ahead logic if requested. */
  if (prefetch)
    readahead(owner, blocknumber);

  std::lock_guard<std::mutex> cachelock(cache_mutex);
  /* If the requested block is already cached, we can return it without IO. */
  if (lookup.count(cache_key)) {
    kio_debug("Serving data key ", *data_key, " for owner ", owner, " from cache.");
    
    /* Splicing the element into the front of the list will keep iterators valid. */
    cache.splice(cache.begin(), cache, lookup[cache_key]);

    /* set owner<->cache_item relationship. Since we have std::sets there's no need to test for existence */
    owner_tables[owner].insert(cache.begin());
    cache.front().owners.insert(owner);
    
    /* Update access timestamp */
    cache.front().last_access = std::chrono::system_clock::now();
    return cache.front().data;
  }
  
  /* Attempt to shrink cache size by releasing unused items */ 
  if(current_size > capacity * 0.7)
    try_shrink();   
  
  /* Re-use an existing data key object if possible, if none exists create a new one. */
  if(unused_items.begin() != unused_items.end()){
    auto it = unused_items.begin();
    unused_size -= it->data->capacity();
    it->owners.clear();
    it->owners.insert(owner);
    it->data->reassign(owner->cluster, data_key, mode);
    it->last_access = std::chrono::system_clock::now();
    cache.splice(cache.begin(), unused_items, it);  
    
  }else{
    cache.push_front(
      CacheItem{ std::set<kio:: FileIo*>{owner}, 
                 std::make_shared<DataBlock>(owner->cluster, data_key, mode), 
                 std::chrono::system_clock::now()
      }
    );
  }
  current_size += cache.front().data->capacity();
  lookup[cache_key] = cache.begin();
  owner_tables[owner].insert(cache.begin());

  kio_debug("Added data key ", *data_key, " to the cache for owner ", owner);
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
  kio().threadpool().run(std::bind(&DataCache::do_flush, this, owner, data));
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
      prefetch.insert(std::make_pair(owner, PrefetchOracle(readahead_window_size)));
    oracle = &prefetch[owner];
    oracle->add(blocknumber);
  }
  
  /* Adjust readahead to cache utilization. Full force to 0.75 usage, decreasing until 0.95, then disabled. */
  size_t readahead_length = readahead_window_size;
  auto cache_utilization = static_cast<double>(current_size) / capacity; 

  if(cache_utilization > 0.95)
    readahead_length = 0;
  else if(cache_utilization > 0.75)
    readahead_length *= ((1.0 - cache_utilization) / 0.25);
  
  if (readahead_length){
    auto prediction = oracle->predict(readahead_length, PrefetchOracle::PredictionType::CONTINUE);
    for (auto it = prediction.cbegin(); it != prediction.cend(); it++) {
      auto data = get(owner, *it, DataBlock::Mode::STANDARD, false);
      auto scheduled = kio().threadpool().try_run(std::bind(do_readahead, data));
      if(scheduled)
        kio_debug("Readahead of data block with identity ", data->getIdentity(), " scheduled for owner ", owner);
    }
  }
}




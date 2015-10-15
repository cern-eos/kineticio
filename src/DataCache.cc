#include "DataCache.hh"
#include "FileIo.hh"
#include "Utility.hh"
#include "Logging.hh"
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

  /* build a vector of bocks, so we can flush without holding cache_mutex */
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
  kio_debug("Removing data key ", *it->data->getKey(), " from cache.");
  for(auto o = it->owners.cbegin(); o!= it->owners.cend(); o++)
    owner_tables[*o].erase(it);
  current_size -= it->data->capacity();
  lookup.erase(*it->data->getKey());
  return cache.erase(it);
}

void DataCache::cache_to_target_size()
{
    using namespace std::chrono;
    if(current_size < target_size){
      write_pressure = 0;
      return; 
    }

    auto expired = system_clock::now() - seconds(5);
    auto num_items = (current_size / cache.back().data->capacity()) * 0.1;
    auto count_items = 0; 
    auto count_dirty = 0; 
        
    kio_debug("Cache current size is ", current_size, " bytes, target size is ", target_size, " bytes. Shrinking cache.");
    for (auto it = --cache.end(); current_size > target_size && num_items > count_items && it != cache.begin(); it--,count_items++) {
      if(it->data->dirty()){
        count_dirty++;
        continue;
      }
      if (it->owners.empty() || it->last_access < expired)
        it = remove_item(it);
    }
    
    write_pressure = count_dirty / static_cast<double>(num_items);
}


void DataCache::throttle()
{
  using namespace std::chrono;
  static const milliseconds ratelimit(50);

  for(auto wait_pressure = 0.01; wait_pressure<write_pressure; wait_pressure += 0.01) 
  {
    {
      std::lock_guard<std::mutex> ratelimit_lock(cache_cleanup_mutex);
      if(duration_cast<milliseconds>(system_clock::now() - cache_cleanup_timestamp) > ratelimit){
        cache_cleanup_timestamp = system_clock::now();
        std::lock_guard<std::mutex> cachelock(cache_mutex);
        cache_to_target_size();      
      }
    }
    /* Sleep 100 ms to give dirty data a chance to flush before retrying */
    usleep(1000 * 100);
  }
}

std::shared_ptr<kio::DataBlock> DataCache::get(
    kio::FileIo* owner, int blocknumber, DataBlock::Mode mode, RequestMode rm)
{
  { std::lock_guard<std::mutex> exeptionlock(exception_mutex);
    if (exceptions.count(owner)) {
      std::exception e = exceptions[owner];
      exceptions.erase(owner);
      throw e;
    }
  }
  
  auto key = utility::constructBlockKey(owner->block_basename, blocknumber);
  
  if(rm == RequestMode::READAHEAD)
    kio_debug("Pre-fetching data key ", *key, " for owner ", owner);
  else 
    kio_debug("Requesting data key ", *key, " for owner ", owner);

  /* If we are called by a client of the cache */
  if(rm == RequestMode::STANDARD){
    /* Register requested block with readahead logic unless we are opening the block for create */
    if (mode != DataBlock::Mode::CREATE)
      readahead(owner, blocknumber);
    /* Throttle this request as indicated by cache pressure */
    throttle();
  }

  std::lock_guard<std::mutex> cachelock(cache_mutex);
  /* If the requested block is already cached, we can return it without IO. */
  if (lookup.count(*key)) {
    /* Splicing the element into the front of the list will keep iterators valid. */
    cache.splice(cache.begin(), cache, lookup[*key]);

    /* set owner<->cache_item relationship. Since we have std::sets there's no need to test for existence */
    owner_tables[owner].insert(cache.begin());
    cache.front().owners.insert(owner);
    cache.front().last_access = std::chrono::system_clock::now();
    return cache.front().data;
  }
  
  /* Attempt to shrink cache size to target size by releasing non-dirty items only*/ 
  cache_to_target_size();   

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

  auto data = std::make_shared<DataBlock>(
      owner->cluster,
      utility::constructBlockKey(owner->block_basename, blocknumber),
      mode
  );
  cache.push_front(CacheItem{std::set<kio:: FileIo*>{owner},data, std::chrono::system_clock::now()});
  lookup[*key] = cache.begin();
  current_size += data->capacity();

  /* set iterator in owner_tables */
  owner_tables[owner].insert(cache.begin());
  
  kio_debug("Adding data key ", *key, " to the cache for owner ", owner);
  return data;
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
  std::list<int> prediction;
  { std::lock_guard<std::mutex> readaheadlock(readahead_mutex);
    if(!prefetch.count(owner))
      prefetch[owner] = PrefetchOracle(readahead_window_size);
    auto& sequence = prefetch[owner];
    sequence.add(blocknumber);
    /* Don't do readahead if cache is full to the brim */
    if (current_size < target_size + owner->cluster->limits().max_value_size)
      prediction = sequence.predict(PrefetchOracle::PredictionType::CONTINUE);
  }
  for (auto it = prediction.cbegin(); it != prediction.cend(); it++) {
    auto data = get(owner, *it, DataBlock::Mode::STANDARD, RequestMode::READAHEAD);
    bg.try_run(std::bind(do_readahead, data));
  }
}




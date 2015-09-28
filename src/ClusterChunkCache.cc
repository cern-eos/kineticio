#include "ClusterChunkCache.hh"
#include "FileIo.hh"
#include "Utility.hh"
#include "Logging.hh"
#include <thread>

using namespace kio;


ClusterChunkCache::ClusterChunkCache(size_t preferred_size, size_t capacity, size_t bg_threads,
                                     size_t bg_queue_depth) :
    target_size(preferred_size), capacity(capacity), current_size(0), bg(bg_threads, bg_queue_depth)
{
  if (capacity < target_size) throw std::logic_error("cache target size may not exceed capacity");
  if (bg_threads < 0) throw std::logic_error("number of background threads cannot be negative.");
}

void ClusterChunkCache::changeConfiguration(size_t preferred_size, size_t cap, size_t bg_threads,
                                            size_t bg_queue_depth)
{
  target_size = preferred_size;
  capacity = cap;
  bg.changeConfiguration(bg_threads, bg_queue_depth);
}

void ClusterChunkCache::drop(kio::FileIo* owner)
{
  /* If we encountered a exception in a background flush, we don't care
     about it if we are dropping the chunk anyways. */
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
      /* Only remove item from cache if there are no other owners left */
      if(!it->owners.size())
        remove_item(it);
    }
  }
  owner_tables.erase(owner);
}

void ClusterChunkCache::flush(kio::FileIo* owner)
{
  /* If we encountered a exception in a background flush, we don't care
     about it, if it is still an isue we will re-encounter it during the flush operation. */
  { std::lock_guard<std::mutex> lock(exception_mutex);
    if (exceptions.count(owner))
      exceptions.erase(owner);
  }

  /* build a vector of chunks, so we can flush without holding cache_mutex */
  std::vector<std::shared_ptr<kio::ClusterChunk> > chunks;
  { std::lock_guard<std::mutex> lock(cache_mutex);
    if (owner_tables.count(owner)) {
      for (auto item = owner_tables[owner].cbegin(); item != owner_tables[owner].cend(); item++) {
        cache_iterator it = *item;
        chunks.push_back(it->chunk);
      }
    }
  }

  for (auto it = chunks.begin(); it != chunks.end(); it++) {
    auto& chunk = *it;
    if (chunk->dirty()) {
      chunk->flush();
    }
  }
}

ClusterChunkCache::cache_iterator ClusterChunkCache::remove_item(const cache_iterator& it)
{
  for(auto o = it->owners.cbegin(); o!= it->owners.cend(); o++)
    owner_tables[*o].erase(it);
  current_size -= it->chunk->capacity();
  lookup.erase(*it->chunk->getKey());
  return cache.erase(it);
}

std::shared_ptr<kio::ClusterChunk> ClusterChunkCache::get(
    kio::FileIo* owner, int chunknumber, ClusterChunk::Mode mode, RequestMode rm)
{
  { std::lock_guard<std::mutex> exeptionlock(exception_mutex);
    if (exceptions.count(owner)) {
      std::exception e = exceptions[owner];
      exceptions.erase(owner);
      throw e;
    }
  }

  if (mode == ClusterChunk::Mode::STANDARD && rm == RequestMode::STANDARD) {
    std::lock_guard<std::mutex> readaheadlock(readahead_mutex);
    auto& sequence = prefetch[owner];
    sequence.add(chunknumber);
    auto prediction = sequence.predict(SequencePatternRecognition::PredictionType::CONTINUE);
    for (auto it = prediction.cbegin(); it != prediction.cend(); it++)
      readahead(owner, *it);
  }

  std::lock_guard<std::mutex> cachelock(cache_mutex);
  auto key = utility::constructChunkKey(owner->chunk_basename, chunknumber);

  /* If the requested chunk is already cached, we can return it without IO. */
  if (lookup.count(*key)) {
    /* Splicing the element into the front of the list will keep iterators valid. */
    cache.splice(cache.begin(), cache, lookup[*key]);

    /* set owner<->cache_item relationship. Since we have std::sets there's no need to test for existance */
    owner_tables[owner].insert(cache.begin());
    cache.front().owners.insert(owner);
    return cache.front().chunk;
  }

  /* try to remove non-dirty items from tail of cache & lookup tables if size > target_size. */
  for (auto it = --cache.end(); current_size > target_size && it != cache.begin(); it--) {
    if (!it->chunk->dirty())
      it = remove_item(it);
  }

  /* If cache size approaches capacity, we have to try flushing dirty chunks manually. */
  if (capacity < current_size + owner->cluster->limits().max_value_size) {
    kio_notice("Cache capacity reached.");
    auto& it = --cache.end();
    if (it->chunk->dirty()) {
      try {
        it->chunk->flush();
      }
      catch (const std::exception& e) {
        throw kio_exception(EIO, "Failed freeing cache space: ", e.what());
        // alternatively, splice into front and retry next cache element....
        // cache.splice(cache.begin(), cache, --cache.end());
        // std::lock_guard<std::mutex> exeptionlock(exception_mutex);
        // exceptions[item.owner] = e;
        // continue;
      }
    }
    remove_item(it);
  }

  auto chunk = std::make_shared<ClusterChunk>(
      owner->cluster,
      utility::constructChunkKey(owner->chunk_basename, chunknumber),
      mode
  );
  cache.push_front(CacheItem{std::set<kio:: FileIo*>{owner},chunk});
  lookup[*key] = cache.begin();
  current_size += chunk->capacity();

  /* set iterator in owner_tables */
  owner_tables[owner].insert(cache.begin());
  return chunk;
}

double ClusterChunkCache::pressure()
{
  if (current_size <= target_size) {
    return 0.0;
  }
  return (current_size - target_size) / static_cast<double>(capacity - target_size);
}

void ClusterChunkCache::do_flush(kio::FileIo* owner, std::shared_ptr<kio::ClusterChunk> chunk)
{
  if (chunk->dirty()) {
    try {
      chunk->flush();
    }
    catch (const std::exception& e) {
      std::lock_guard<std::mutex> lock(exception_mutex);
      exceptions[owner] = e;
    }
  }
}

void ClusterChunkCache::async_flush(kio::FileIo* owner, std::shared_ptr<ClusterChunk> chunk)
{
  bg.run(std::bind(&ClusterChunkCache::do_flush, this, owner, chunk));
}

void do_readahead(std::shared_ptr<kio::ClusterChunk> chunk)
{
  /* if readahaed should throw, there's no need to remember as in do_flush...
   * we'll just re-encounter the exception should the chunk actually be read from */
  char buf[1];
  chunk->read(buf, 0, 1);
}

void ClusterChunkCache::readahead(kio::FileIo* owner, int chunknumber)
{
  /* Don't do readahead if cache is already under pressure. */
  if (pressure() > 0.1) {
    return;
  }
  auto chunk = get(owner, chunknumber, ClusterChunk::Mode::STANDARD, RequestMode::READAHEAD);
  bg.try_run(std::bind(do_readahead, chunk));
}




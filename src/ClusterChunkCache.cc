#include "ClusterChunkCache.hh"
#include "FileIo.hh"
#include "Utility.hh"
#include <thread>
#include <LoggingException.hh>

using namespace kio;


ClusterChunkCache::ClusterChunkCache(size_t preferred_size, size_t capacity, size_t threads) :
    target_size(preferred_size), capacity(capacity), current_size(0), bg(threads)
{
  if(capacity<target_size) throw std::logic_error("cache target size may not exceed capacity");
  if(threads<0) throw std::logic_error("number of background threads cannot be negative.");
}

void ClusterChunkCache::drop(kio::FileIo* owner)
{
  /* If we encountered a exception in a background flush, we don't care
     about it if we are dropping the chunk anyways. */
  {
    std::lock_guard<std::mutex> lock(exception_mutex);
    if (exceptions.count(owner)) {
      exceptions.erase(owner);
    }
  }

  {
    std::lock_guard<std::mutex> lock(readahead_mutex);
    if (prefetch.count(owner)) {
      prefetch.erase(owner);
    }
  }

  std::lock_guard<std::mutex> lock(cache_mutex);
  if (lookup.count(owner)) {
    auto& chunkmap = lookup[owner];

    for (auto it = chunkmap.cbegin(); it != chunkmap.cend(); it++) {
      current_size -= it->second->chunk->capacity();
      cache.erase(it->second);
    }
    lookup.erase(owner);
  }
}

void ClusterChunkCache::flush(kio::FileIo* owner)
{
  {
    std::lock_guard<std::mutex> lock(exception_mutex);
    if (exceptions.count(owner)) {
      exceptions.erase(owner);
    }
  }

  std::vector<std::shared_ptr<kio::ClusterChunk> > chunks;
  {
    std::lock_guard<std::mutex> lock(cache_mutex);

    if (lookup.count(owner)) {
      auto& chunkmap = lookup[owner];
      for (auto it = chunkmap.cbegin(); it != chunkmap.cend(); it++) {
        chunks.push_back(it->second->chunk);
      }
    }
  }

  for (auto it = chunks.begin(); it != chunks.end(); it++) {
    if ((*it)->dirty()) {
      (*it)->flush();
    }
  }
}

std::shared_ptr<kio::ClusterChunk> ClusterChunkCache::get(
    kio::FileIo* owner, int chunknumber, ClusterChunk::Mode mode, RequestMode rm)
{
  {
    std::lock_guard<std::mutex> exeptionlock(exception_mutex);
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
  auto& chunkmap = lookup[owner];

  /* If the requested chunk is already cached, we can return it without IO. */
  if (chunkmap.count(chunknumber)) {
    /* Splicing the element into the front of the list will keep iterators
       valid. We only have to take care that we are not trying to splice it on
       itself. */
    if (chunkmap[chunknumber] != cache.begin()) {
      cache.splice(cache.begin(), cache, chunkmap[chunknumber]);
    }
    return cache.front().chunk;
  }

  /* try to remove non-dirty items from tail of cache & lookup tables if size > target_size. */
  for(auto it = --cache.end(); current_size > target_size && it != cache.begin(); it--){
    if(!it->chunk->dirty()){
      current_size -= it->chunk->capacity();
      lookup[it->owner].erase(it->id);
      it = cache.erase(it);
    }
  }

  /* If cache size approaches capacity, we have to try flushing dirty chunks manually. */
  if (capacity < current_size+owner->cluster->limits().max_value_size) {
    auto& item = cache.back();
    if (item.chunk->dirty()) {
      try {
        item.chunk->flush();
      }
      catch (const std::exception& e) {
          throw LoggingException(EIO, __FUNCTION__, __FILE__, __LINE__,
                  "Failed freeing cache space:" + std::string(e.what())
          );

       // alternatively, splice into front and retry next cache element....
       // cache.splice(cache.begin(), cache, --cache.end());
       // std::lock_guard<std::mutex> exeptionlock(exception_mutex);
       // exceptions[item.owner] = e;
       // continue;
      }
    }
    current_size -= item.chunk->capacity();
    lookup[item.owner].erase(item.id);
    cache.pop_back();
  }

  auto chunk = std::make_shared<ClusterChunk>(
      owner->cluster,
      utility::constructChunkKey(owner->chunk_basename, chunknumber),
      mode
  );
  cache.push_front(CacheItem{chunknumber, owner, chunk});
  chunkmap[chunknumber] = cache.begin();
  current_size += chunk->capacity();
  return chunk;
}

double ClusterChunkCache::pressure()
{
  std::lock_guard<std::mutex> cachelock(cache_mutex);
  if(current_size <= target_size)
    return 0.0;
  return (current_size-target_size) / (double) (capacity-target_size);
}

void ClusterChunkCache::do_flush(kio::FileIo *owner, std::shared_ptr<kio::ClusterChunk> chunk)
{
  if (chunk->dirty()) {
    try {
      chunk->flush();
    }
    catch (const std::exception &e) {
      std::lock_guard<std::mutex> lock(exception_mutex);
      exceptions[owner] = e;
    }
  }
}

void ClusterChunkCache::async_flush(kio::FileIo* owner, std::shared_ptr<ClusterChunk> chunk)
{
  auto fun = std::bind(&ClusterChunkCache::do_flush, this, owner, chunk);
  bg.run(fun);
}

void do_readahead(std::shared_ptr<kio::ClusterChunk> chunk)
{
  char buf[1];
  chunk->read(buf, 0, 1);
}

void ClusterChunkCache::readahead(kio::FileIo* owner, int chunknumber)
{
  auto chunk = get(owner, chunknumber, ClusterChunk::Mode::STANDARD, RequestMode::READAHEAD);
  auto fun = std::bind(do_readahead, chunk);
  bg.try_run(fun);
}




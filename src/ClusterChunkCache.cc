#include "ClusterChunkCache.hh"
#include "FileIo.hh"
#include "Utility.hh"
#include <thread>

using namespace kio;


ClusterChunkCache::ClusterChunkCache(size_t capacity, size_t threads) :
    capacity(capacity), bg(threads)
{
}

ClusterChunkCache::~ClusterChunkCache()
{
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

    for (auto it = chunkmap.begin(); it != chunkmap.end(); it++) {
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
      for (auto it = chunkmap.begin(); it != chunkmap.end(); it++) {
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

  /* If size >= capacity, results last item from cache & lookup tables. */
  while (cache.size() >= capacity) {
    auto& item = cache.back();
    if (item.chunk->dirty()) {
      try {
        item.chunk->flush();
      }
      catch (const std::exception& e) {
        cache.splice(cache.begin(), cache, --cache.end());
        std::lock_guard<std::mutex> exeptionlock(exception_mutex);
        exceptions[item.owner] = e;
        continue;
      }
    }
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
  return chunk;
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

void ClusterChunkCache::flush(kio::FileIo* owner, std::shared_ptr<ClusterChunk> chunk)
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




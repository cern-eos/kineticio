#include "ClusterChunkCache.hh"

#include "FileIo.hh"

#include "Utility.hh"
#include <thread>
#include <string>
#include <condition_variable>

using namespace kio;


ClusterChunkCache::ClusterChunkCache(size_t capacity, size_t threads) :
  item_capacity(capacity), thread_capacity(threads), numthreads(0)
{
}

ClusterChunkCache::~ClusterChunkCache()
{
  while(numthreads.load()){
  }
}


void ClusterChunkCache::drop(kio::FileIo* owner)
{
  /* If we encountered a exception in a background flush, we don't care
     about it if we are dropping the chunk anyways. */
  {
    std::lock_guard<std::mutex> lock(exception_mutex);
    if(exceptions.count(owner))
      exceptions.erase(owner);
  }

  std::lock_guard<std::mutex> lock(cache_mutex);

  if(lookup.count(owner)){
    auto& chunkmap = lookup[owner];

    for(auto it = chunkmap.begin(); it != chunkmap.end(); it++){
        cache.erase(it->second);
    }
    lookup.erase(owner);
  }
}

void ClusterChunkCache::flush(kio::FileIo* owner)
{
  {
    std::lock_guard<std::mutex> lock(exception_mutex);
    if(exceptions.count(owner))
      exceptions.erase(owner);
  }

  std::vector< std::shared_ptr<kio::ClusterChunk> > chunks;
  {
    std::lock_guard<std::mutex> lock(cache_mutex);

    if(lookup.count(owner)){
      auto& chunkmap = lookup[owner];

      for(auto it = chunkmap.begin(); it != chunkmap.end(); it++){
          chunks.push_back(it->second->chunk);
      }
    }
  }

  for(auto it = chunks.begin(); it != chunks.end(); it++){
    if((*it)->dirty())
      (*it)->flush();
  }
}

std::shared_ptr<kio::ClusterChunk> ClusterChunkCache::get(
  kio::FileIo* owner, int chunknumber, ChunkMode mode)
{
  {
    std::lock_guard<std::mutex> lock(exception_mutex);
    if(exceptions.count(owner)){
      std::exception e = exceptions[owner];
      exceptions.erase(owner);
      throw e;
    }
  }

  std::lock_guard<std::mutex> lock(cache_mutex);
  auto& chunkmap = lookup[owner];

  /* If the requested chunk is already cached, we can return it without IO. */
  if(chunkmap.count(chunknumber)){
    /* Splicing the element into the front of the list will keep iterators
       valid. We only have to take care that we are not trying to splice it on
       itself. */
    if(chunkmap[chunknumber] != cache.begin()){
      cache.splice( cache.begin(), cache, chunkmap[chunknumber] );
    }
    return cache.front().chunk;
  }

  /* If size >= capacity, remove last item from cache & lookup tables. */
  while(cache.size() >= item_capacity){
    auto& item = cache.back();
    if(item.chunk->dirty()){
      try{
        item.chunk->flush();
      }
      catch(const std::exception& e){
          cache.splice( cache.begin(), cache, chunkmap[chunknumber] );
          std::lock_guard<std::mutex> lock(exception_mutex);
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

void ClusterChunkCache::threadsafe_readahead(kio::FileIo* owner, std::shared_ptr<kio::ClusterChunk> chunk)
{
  numthreads++;
  try{
    if(chunk->dirty()){
      char buf[1];
      chunk->read(buf, 0, 1);
    }
  }
  catch(...){}
  numthreads--;
}

void ClusterChunkCache::threadsafe_flush(kio::FileIo* owner, std::shared_ptr<kio::ClusterChunk> chunk)
{
  numthreads++;
  try{
    if(chunk->dirty()){
      try{
        chunk->flush();
      }
      catch(const std::exception& e){
        std::lock_guard<std::mutex> lock(exception_mutex);
        exceptions[owner] = e;
      }
    }
  }
  catch(...){}
  numthreads--;
}

void ClusterChunkCache::flush(kio::FileIo* owner, std::shared_ptr<ClusterChunk> chunk)
{
  if(numthreads.load() < thread_capacity)
    std::thread(&ClusterChunkCache::threadsafe_flush, this, owner, chunk).detach();
  else
    threadsafe_flush(owner,chunk);
}

void ClusterChunkCache::readahead(kio::FileIo* owner, int chunknumber)
{
  if(numthreads.load() < thread_capacity){
    auto chunk = get(owner, chunknumber, ChunkMode::standard);
    std::thread(&ClusterChunkCache::threadsafe_readahead, this, owner, chunk).detach();
  }
}




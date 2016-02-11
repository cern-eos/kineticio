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

#include "DataCache.hh"
#include "FileIo.hh"
#include "Utility.hh"
#include "Logging.hh"
#include "KineticCluster.hh"
#include "KineticIoSingleton.hh"
#include <thread>
#include <unistd.h>

using namespace kio;


DataCache::DataCache(size_t capacity) :
    capacity(capacity), current_size(0), unused_size(0)
{
}

void DataCache::changeConfiguration(size_t cap)
{
  capacity = cap;
}

void DataCache::drop(kio::FileIo* owner, bool force)
{
  std::lock_guard<std::mutex> lock(cache_mutex);
  if (owner_tables.count(owner)) {
    for (auto owit = owner_tables[owner].cbegin(); owit != owner_tables[owner].cend(); owit++) {
      cache_iterator it = *owit;
      it->owners.erase(owner);
      /* Because some clients apparently like re-opening files, we will no longer automatically remove orphaned 
       * data keys (unless force is set)... they will only be removed when cache pressure indicates.  */
      if (force) {
        remove_item(it);
      }
    }
  }
  owner_tables.erase(owner);
}

void DataCache::flush(kio::FileIo* owner)
{
  /* build a vector of blocks, so we can flush without holding cache_mutex */
  std::vector<std::shared_ptr<kio::DataBlock> > blocks;
  {
    std::lock_guard<std::mutex> lock(cache_mutex);
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
  for (auto o = it->owners.cbegin(); o != it->owners.cend(); o++) {
    owner_tables[*o].erase(it);
  }

  lookup.erase(it->data->getIdentity());
  current_size -= it->data->capacity();

  /* We don't want to keep too many unused cache items around... */
  if (unused_size > 0.1 * capacity) {
    kio_debug("Deleting cache key ", it->data->getIdentity(), " from cache.");
    return cache.erase(it);
  }

  kio_debug("Transferring cache key ", it->data->getIdentity(), " from cache to unused items pool.");
  auto next_it = std::next(it);
  unused_size += it->data->capacity();
  unused_items.splice(unused_items.begin(), cache, it);
  return next_it;
}

namespace {
  void doFlush(std::shared_ptr<kio::DataBlock> data)
  {
    if (data->dirty()) {
      data->flush();
    }
  }
}

void DataCache::try_shrink()
{
  using namespace std::chrono;
  auto expired = system_clock::now() - seconds(5);
  auto num_items = (current_size / cache.back().data->capacity()) * 0.1;
  auto count_items = 0;

  for (auto it = --cache.end(); num_items > count_items && it != cache.begin(); it--, count_items++) {
    if ((it->owners.empty() || it->last_access < expired) && !it->data->dirty() && it->data.unique()) {
      it = remove_item(it);
    }
    else if(it->data->dirty() && it->last_access < expired){
      kio().threadpool().try_run(std::bind(&doFlush, it->data));
    }
  }

  /* If cache size exceeds capacity, we have to force remove data keys. */
  if (capacity < current_size) {
    kio_debug("Cache capacity reached.");

    for (auto it = --cache.end(); capacity < current_size && it != cache.begin(); it--) {
      if (it->data.unique() && !it->data->dirty()) {
        kio_debug("Cache key ", it->data->getIdentity(), " identified for removal. It is in cache position ",
                  std::distance(cache.begin(), it), " out of ", std::distance(cache.begin(), cache.end()),
                  " and has last been accessed ", duration_cast<seconds>(system_clock::now() - it->last_access), " ago");
        remove_item(it);
      }
    }

    /* Second try.. can't find an ideal candidate, we will flush. */
    for (auto it = --cache.end(); capacity < current_size && it != cache.begin(); it--) {
      if (it->data.unique()) {
        if (it->data->dirty()) {
          try {
            it->data->flush();
          }
          catch (const std::exception& e) {
            kio_warning("Failed flushing cache item ", it->data->getIdentity(), "  Reason: ", e.what());
            continue;
          }
        }
        kio_notice("Cache key ", it->data->getIdentity(), " identified for FORCE REMOVAL as there were no clean unique"
            "keys in the cache to drop.");
        remove_item(it);
      }
    }
  }
}

std::shared_ptr<kio::DataBlock> DataCache::get(kio::FileIo* owner, int blocknumber, DataBlock::Mode mode)
{
  /* We cannot use the block key directly for cache lookups, as reloading the configuration will create
     different cluster objects and we have to avoid FileIo objects being associated with multiple clusters */
  auto data_key = utility::makeDataKey(owner->cluster->id(), owner->path, blocknumber);
  std::string cache_key = *data_key + owner->cluster->instanceId();

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
  if (current_size > capacity * 0.7) {
    try_shrink();
  }

  /* Re-use an existing data key object if possible, if none exists create a new one. */
  if (unused_items.begin() != unused_items.end()) {
    auto it = unused_items.begin();
    unused_size -= it->data->capacity();
    it->owners.clear();
    it->owners.insert(owner);
    it->data->reassign(owner->cluster, data_key, mode);
    it->last_access = std::chrono::system_clock::now();
    cache.splice(cache.begin(), unused_items, it);
  }
  else {
    cache.push_front(
        CacheItem{std::set<kio::FileIo*>{owner},
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

double DataCache::utilization()
{
  return static_cast<double>(current_size) / capacity;
}

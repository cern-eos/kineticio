#include <thread>

#include "KineticFileIo.hh"
#include "KineticFileAttr.hh"
#include "KineticClusterMap.hh"
#include "KineticException.hh"
#include "PathUtil.hh"

using std::shared_ptr;
using std::unique_ptr;
using std::string;
using std::make_shared;
using std::chrono::system_clock;
using kinetic::KineticStatus;
using kinetic::StatusCode;

KineticFileIo::KineticFileIo (size_t cache_capacity) :
    cluster(), cache(*this, cache_capacity), lastChunkNumber(*this)
{
}

KineticFileIo::~KineticFileIo ()
{
}

/* All necessary checks have been done in the 993 line long
 * XrdFstOfsFile::open method before we are called. */
void KineticFileIo::Open (const std::string& p, int flags,
		mode_t mode, const std::string& opaque, uint16_t timeout)
{
  cluster = cmap().getCluster(path_util::extractID(p));

  /* Setting path variables. There is no need to encode kinetic:clusterID in
   * all chunk keys. */
  obj_path = p;
  chunk_basename = obj_path.substr(obj_path.find_last_of(':')+1, obj_path.length());

  /* Put the metadata key... if it already exists the operation will fail with
   * a version missmatch error, which is fine... */
  shared_ptr<const string> version_out;
  KineticStatus s = cluster->put(make_shared<string>(obj_path),
          make_shared<const string>(), make_shared<const string>(), false,
          version_out);
  if(s.ok())
    lastChunkNumber.set(0);
  else if(s.statusCode() != StatusCode::REMOTE_VERSION_MISMATCH)
    throw KineticException(EIO,__FUNCTION__,__FILE__,__LINE__,
            "Attempting to write metadata key '"+ obj_path +"' to cluster "
            "returned unexpected error: '"+s.message()+"'");
}

void KineticFileIo::Close (uint16_t timeout)
{
  Sync(timeout);
  cluster.reset();
  obj_path.clear();
  chunk_basename.clear();
}

int64_t KineticFileIo::ReadWrite (long long off, char* buffer,
		int length, KineticFileIo::rw mode, uint16_t timeout)
{
  if(!cluster) throw KineticException(ENXIO,__FUNCTION__,__FILE__,__LINE__,
                              "No cluster set for FileIO object.");

  const size_t chunk_capacity = cluster->limits().max_value_size;
  size_t length_todo = length;
  off_t off_done = 0;

  while(length_todo){
    int chunk_number = (off+off_done) / chunk_capacity;
    off_t chunk_offset = (off+off_done) - chunk_number * chunk_capacity;
    size_t chunk_length = std::min(length_todo, chunk_capacity - chunk_offset);

   /* Increase last chunk number if we write past currently known file size...
    * also assume the chunk doesn't exist yet in this case.  */
    bool create=false;
    if(chunk_number > lastChunkNumber.get() && (mode == rw::WRITE)){
      lastChunkNumber.set(chunk_number);
      create = true;
    }
    shared_ptr<KineticChunk> chunk = cache.get(chunk_number, create);

    if(mode == rw::WRITE){
      chunk->write(buffer+off_done, chunk_offset, chunk_length);

      /* Flush chunk in background if writing to chunk capacity. Do not worry
       * about creating too many threads: There can never be more
       * chunks in memory than the size of the chunk cache, which implicitly
       * limits the number of threads that could be concurrently created here.*/
      if(chunk_offset + chunk_length == chunk_capacity)
        std::thread(&KineticChunk::flush, chunk).detach();
    }
    else if (mode == rw::READ){
      chunk->read(buffer+off_done, chunk_offset, chunk_length);

      /* If we are reading the last chunk (or past it) */
      if(chunk_number >= lastChunkNumber.get()){
        /* make sure length doesn't indicate that we read past filesize. */
        if(chunk->size() > chunk_offset)
          length_todo -= std::min(chunk_length,
                   (size_t) chunk->size() - chunk_offset);
        break;
      }
    }
    length_todo -= chunk_length;
    off_done += chunk_length;
  }

  return length-length_todo;
}

int64_t KineticFileIo::Read (long long offset, char* buffer, int length,
      uint16_t timeout)
{
  return ReadWrite(offset, buffer, length, KineticFileIo::rw::READ, timeout);
}

int64_t KineticFileIo::Write (long long offset, const char* buffer,
      int length, uint16_t timeout)
{
  return ReadWrite(offset, const_cast<char*>(buffer), length,
          KineticFileIo::rw::WRITE, timeout);
}

void KineticFileIo::Truncate (long long offset, uint16_t timeout)
{
  if(!cluster) throw KineticException(ENXIO,__FUNCTION__,__FILE__,__LINE__,
                              "No cluster set for FileIO object.");

  const size_t chunk_capacity = cluster->limits().max_value_size;
  int chunk_number = offset /chunk_capacity;
  int chunk_offset = offset - chunk_number * chunk_capacity;

  /* Step 1) truncate the chunk containing the offset. */
  cache.get(chunk_number)->truncate(chunk_offset);

  /* Step 2) Ensure we don't have chunks past chunk_number in the cache. Since
   * truncate isn't super common, go the easy way and just sync+drop the
   * cache... this will also sync the just truncated chunk.  */
  Sync();
  cache.clear();

  /* Step 3) Delete all chunks past chunk_number. When truncating to size 0,
   * (and only then) also delete the first chunk. */
  std::unique_ptr<std::vector<string>> keys;
  const size_t max_keys_requested = 100;
  do{
    KineticStatus status = cluster->range(
            path_util::chunkKey(chunk_basename, offset ? chunk_number+1 : 0),
            path_util::chunkKey(chunk_basename, 99999999),
            max_keys_requested, keys);
    if(!status.ok())
      throw KineticException(EIO,__FUNCTION__,__FILE__,__LINE__,
              "KeyRange request unexpectedly failed for object "+obj_path+"': "
              +status.message());

    for (auto iter = keys->begin(); iter != keys->end(); ++iter){
      status = cluster->remove(make_shared<string>(*iter),
              make_shared<string>(""), true);
      if(!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND)
        throw KineticException(EIO,__FUNCTION__,__FILE__,__LINE__,
                "Deleting chunk " + *iter +" failed: "+status.message());
    }
  }while(keys->size() == max_keys_requested);

  /* Set last chunk number */
  lastChunkNumber.set(chunk_number);
}

void KineticFileIo::Remove (uint16_t timeout)
{
  Truncate(0);
  KineticStatus status = cluster->remove(make_shared<string>(obj_path),
          make_shared<string>(), true);
  if(!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND)
    throw KineticException(EIO,__FUNCTION__,__FILE__,__LINE__,
            "Could not delete metdata key " +obj_path+ ": "+status.message());
}

void KineticFileIo::Sync (uint16_t timeout)
{
  if(!cluster) throw KineticException(ENXIO,__FUNCTION__,__FILE__,__LINE__,
         "No cluster set for FileIO object.");

  cache.flush();
}

void KineticFileIo::Stat (struct stat* buf, uint16_t timeout)
{
  if(!cluster) throw KineticException(ENXIO,__FUNCTION__,__FILE__,__LINE__,
         "No cluster set for FileIO object.");

  lastChunkNumber.verify();
  std::shared_ptr<KineticChunk> last_chunk = cache.get(lastChunkNumber.get());

  memset(buf, 0, sizeof(struct stat));
  buf->st_blksize = cluster->limits().max_value_size;
  buf->st_blocks = lastChunkNumber.get() + 1;
  buf->st_size = lastChunkNumber.get() * buf->st_blksize + last_chunk->size();
}


void KineticFileIo::Statfs (const char* p, struct statfs* sfs)
{
  if(obj_path.length() && obj_path.compare(p))
    throw KineticException(EINVAL,__FUNCTION__,__FILE__,__LINE__,
         "Object concurrently used for both Statfs and FileIO");
 
  if(!cluster){
    cluster = cmap().getCluster(path_util::extractID(p));
    obj_path=p;
  }

  KineticClusterSize s;
  if(!cluster->size(s).ok())
     throw KineticException(EIO,__FUNCTION__,__FILE__,__LINE__,
         "Could not obtain cluster size values.");

  /* Minimal allocated block size. Set to 4K because that's the
   * maximum accepted value by Linux. */
  sfs->f_frsize = 4096;
   /* Preferred file system block size for I/O requests. This is sometimes
    * evaluated as the actual block size (e.g. by EOS). We set the bsize equal
    * to the frsize to avoid confusion. This approach is also taken by all
    * kernel level file systems. */
  sfs->f_bsize  = sfs->f_frsize;
  /* Blocks on FS in units of f_frsize */
  sfs->f_blocks = (fsblkcnt_t) (s.bytes_total / sfs->f_frsize);
  /* Free blocks */
  sfs->f_bavail = (fsblkcnt_t) (s.bytes_free / sfs->f_frsize);
  /* Free blocks available to non root user */
  sfs->f_bfree  = sfs->f_bavail;
  /* Total inodes. */
  sfs->f_files   = s.bytes_total;
  /* Free inodes */
  sfs->f_ffree   = s.bytes_free;
}

struct ftsState{
  std::unique_ptr<std::vector<string>> keys;
  shared_ptr<string> end_key;
  size_t index;

  ftsState(std::string subtree):keys(new std::vector<string>({subtree})),index(1)
  {
    end_key = make_shared<string>(subtree+"~");
  }
};

void* KineticFileIo::ftsOpen(std::string subtree)
{
  cluster = cmap().getCluster(path_util::extractID(subtree));
  return new ftsState(subtree);
}

std::string KineticFileIo::ftsRead(void* fts_handle)
{
  if(!fts_handle) return "";
  ftsState * state = (ftsState*) fts_handle;

  if(state->keys->size() <= state->index){
    const size_t max_key_requests = 100;
    state->index = 0;
    /* add a space character (lowest ascii printable) to make the range request
       non-including. */
    cluster->range(
      make_shared<string>(state->keys->back()+" "),
      state->end_key,
      max_key_requests, state->keys
    );
  }
  return state->keys->empty() ? "" : state->keys->at(state->index++);
}

int KineticFileIo::ftsClose(void* fts_handle)
{
  ftsState * state = (ftsState*) fts_handle;
  if(state){
    delete state;
    return 0;
  }
  return -1;
}

KineticFileIo::LastChunkNumber::LastChunkNumber(KineticFileIo & parent) : 
            parent(parent), last_chunk_number(0), last_chunk_number_timestamp()
{}

KineticFileIo::LastChunkNumber::~LastChunkNumber()
{}

int KineticFileIo::LastChunkNumber::get() const
{
  return last_chunk_number;
}

void KineticFileIo::LastChunkNumber::set(int chunk_number)
{
  last_chunk_number = chunk_number;
  last_chunk_number_timestamp = system_clock::now();
}

void KineticFileIo::LastChunkNumber::verify()
{
  /* chunk number verification independent of standard expiration verification
   * in KinetiChunk class. validate last_chunk_number (another client might have
   * created new chunks we know nothing about, or truncated the file. */
  if( std::chrono::duration_cast<std::chrono::milliseconds>(
        system_clock::now() - last_chunk_number_timestamp).count()
      < KineticChunk::expiration_time
    ) return;

  /* Technically, we could start at chunk 0 to catch all cases... but that the
   * file is truncated by another client while opened here is highly unlikely.
   * And for big files this would mean unnecessary GetKeyRange requests for the
   * regular case.  */
  const size_t max_keys_requested = 100;
  std::unique_ptr<std::vector<string>> keys;
  do{
    KineticStatus status = parent.cluster->range(keys ?
            make_shared<const string>(keys->back()) :
            path_util::chunkKey(parent.chunk_basename, last_chunk_number),
            path_util::chunkKey(parent.chunk_basename, 99999999),
            max_keys_requested,
            keys);

    if(!status.ok())
       throw KineticException(EIO,__FUNCTION__,__FILE__,__LINE__,
              "KeyRange request unexpectedly failed for chunks with base name: "
              +parent.chunk_basename+": "+status.message());
  }while(keys->size() == max_keys_requested);

  /* Success: get chunk number from last key.*/
  if(keys->size() > 0){
    std::string key = keys->back();
    std::string number = key.substr(key.find_last_of('_')+1, key.length());
    set(std::stoll(number));
    return;
  }

  /* No keys found. the file might have been truncated, retry but start the
   * search from chunk 0 this time. */
  if(last_chunk_number > 0){
      last_chunk_number = 0;
      return verify();
  }

  /* No chunk keys found. Ensure that the key has not been removed by testing for
     the existence of the metadata key. */
  shared_ptr<const string> version;
  shared_ptr<const string> value;
  auto status = parent.cluster->get(
          make_shared<const string>(parent.obj_path),
          true, version, value);
  if(!status.ok())
    throw KineticException(ENOENT,__FUNCTION__,__FILE__,__LINE__,
              "File "+parent.obj_path+" does not exist.");
}


KineticFileIo::KineticChunkCache::KineticChunkCache(KineticFileIo & parent, size_t cache_capacity):
    parent(parent), capacity(cache_capacity)
{
}

KineticFileIo::KineticChunkCache::~KineticChunkCache()
{
}

void KineticFileIo::KineticChunkCache::clear()
{
  cache.clear();
  lru_order.clear();
}

void KineticFileIo::KineticChunkCache::flush()
{
  for(auto it=cache.begin(); it!=cache.end(); ++it){
    if(it->second->dirty())
      it->second->flush();
  }
}

std::shared_ptr<KineticChunk> KineticFileIo::KineticChunkCache::get(int chunk_number, bool create)
{
  if(cache.count(chunk_number)){
    lru_order.remove(chunk_number);
    lru_order.push_back(chunk_number);
    return cache.at(chunk_number);
  }

  if(lru_order.size() >= capacity){
    if(cache.at(lru_order.front())->dirty())
      cache.at(lru_order.front())->flush();
    cache.erase(lru_order.front());
    lru_order.pop_front();
  }

  std::shared_ptr<KineticChunk> chunk(new KineticChunk(parent.cluster,
      path_util::chunkKey(parent.chunk_basename, chunk_number), create));
  cache.insert(std::make_pair(chunk_number,chunk));
  lru_order.push_back(chunk_number);
  return chunk;
}
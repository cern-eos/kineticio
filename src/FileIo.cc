#include <unistd.h>
#include <Logging.hh>
#include "FileIo.hh"
#include "ClusterMap.hh"
#include "LoggingException.hh"
#include "Utility.hh"

using std::shared_ptr;
using std::unique_ptr;
using std::string;
using std::make_shared;
using std::chrono::system_clock;
using kinetic::KineticStatus;
using kinetic::StatusCode;

using namespace kio;


FileIo::FileIo() :
    cluster(), cache(ClusterMap::getInstance().getCache()), lastChunkNumber(*this)
{
}

FileIo::~FileIo()
{
}

/* All necessary checks have been done in the 993 line long
 * XrdFstOfsFile::open method before we are called. */
void FileIo::Open(const std::string &p, int flags,
                  mode_t mode, const std::string &opaque, uint16_t timeout)
{
  /* TODO: throttle on number of open files? */
  cluster = ClusterMap::getInstance().getCluster(utility::extractClusterID(p));

  /* Setting path variables. There is no need to encode kinetic:clusterID in
   * all chunk keys. */
  obj_path = p;
  chunk_basename = obj_path.substr(obj_path.find_last_of(':') + 1, obj_path.length());

  /* Put the metadata key... if it already exists the operation will fail with
   * a version missmatch error, which is fine... */
  shared_ptr<const string> version_out;
  auto s = cluster->put(
      make_shared<string>(obj_path),
      make_shared<const string>(),
      make_shared<const string>(),
      false,
      version_out);

  if (s.ok())
    lastChunkNumber.set(0);
  else if (s.statusCode() != StatusCode::REMOTE_VERSION_MISMATCH)
    throw kio_exception(EIO, "Attempting to write metadata key '",  obj_path, "' to cluster returned unexpected error ", s);


void FileIo::Close(uint16_t timeout)
{
  Sync(timeout);
  cache.drop(this);
  cluster.reset();
  obj_path.clear();
  chunk_basename.clear();
}


int64_t FileIo::ReadWrite(long long off, char *buffer,
                          int length, FileIo::rw mode, uint16_t timeout)
{
  if (!cluster)
    throw kio_exception(ENXIO, "No cluster set for FileIO object ", obj_path);

  /* Delay response in case of cache pressure in order to throttle requests in high
   * pressure scenarios. For now, we simply delay for a percentage of the timeout time. */
  int delay;
  do{
    delay = (timeout ? timeout : 60) * cache.pressure();
    if(delay) sleep(delay);
    if(timeout) timeout -= delay;
  }while(delay);

  const size_t chunk_capacity = cluster->limits().max_value_size;
  size_t length_todo = length;
  off_t off_done = 0;

  while (length_todo) {
    int chunk_number = static_cast<int>((off + off_done) / chunk_capacity);
    off_t chunk_offset = (off + off_done) - chunk_number * chunk_capacity;
    size_t chunk_length = std::min(length_todo, chunk_capacity - chunk_offset);

    /* Increase last chunk number if we write past currently known file size...*/
    ClusterChunk::Mode cm = ClusterChunk::Mode::STANDARD;
    if (mode == rw::WRITE && chunk_number > lastChunkNumber.get()) {
      lastChunkNumber.set(chunk_number);
      cm = ClusterChunk::Mode::CREATE;
    }
    auto chunk = cache.get(this, chunk_number, cm);

    if (mode == rw::WRITE) {
      chunk->write(buffer + off_done, chunk_offset, chunk_length);

      /* Flush chunk in background if writing to chunk capacity.*/
      if (chunk_offset + chunk_length == chunk_capacity)
        cache.async_flush(this, chunk);
    }
    else if (mode == rw::READ) {
      chunk->read(buffer + off_done, chunk_offset, chunk_length);

      /* If we are reading the last chunk (or past it) */
      if (chunk_number >= lastChunkNumber.get()) {
        /* make sure length doesn't indicate that we read past filesize. */
        if (chunk->size() > chunk_offset)
          length_todo -= std::min(chunk_length,
                                  (size_t) chunk->size() - chunk_offset);
        break;
      }
    }
    length_todo -= chunk_length;
    off_done += chunk_length;
  }

  return length - length_todo;
}

int64_t FileIo::Read(long long offset, char *buffer, int length,
                     uint16_t timeout)
{
  return ReadWrite(offset, buffer, length, FileIo::rw::READ, timeout);
}

int64_t FileIo::Write(long long offset, const char *buffer,
                      int length, uint16_t timeout)
{
  return ReadWrite(offset, const_cast<char *>(buffer), length,
                   FileIo::rw::WRITE, timeout);
}

void FileIo::Truncate(long long offset, uint16_t timeout)
{
  if (!cluster)
    throw kio_exception(ENXIO, "No cluster set for FileIO object ", obj_path);

  const size_t chunk_capacity = cluster->limits().max_value_size;
  int chunk_number = offset / chunk_capacity;
  int chunk_offset = offset - chunk_number * chunk_capacity;

  /* Step 1) truncate the chunk containing the offset. */
  cache.get(this, chunk_number, ClusterChunk::Mode::STANDARD)->truncate(chunk_offset);

  /* Step 2) Ensure we don't have chunks past chunk_number in the cache. Since
   * truncate isn't super common, go the easy way and just sync+drop the
   * cache... this will also sync the just truncated chunk.  */
  cache.flush(this);
  cache.drop(this);

  /* Step 3) Delete all chunks past chunk_number. When truncating to size 0,
   * (and only then) also delete the first chunk. */
  std::unique_ptr<std::vector<string>> keys;
  const size_t max_keys_requested = 100;
  do {
    KineticStatus status = cluster->range(
        utility::constructChunkKey(chunk_basename, offset ? chunk_number + 1 : 0),
        utility::constructChunkKey(chunk_basename, std::numeric_limits<int>::max()),
        max_keys_requested, keys);
    if (!status.ok())
      throw kio_exception(EIO, "KeyRange request unexpectedly failed for object ",  obj_path,  ": ", status);

    for (auto iter = keys->begin(); iter != keys->end(); ++iter) {
      status = cluster->remove(make_shared<string>(*iter),
                               make_shared<string>(""), true);
      if (!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND)
        throw kio_exception(EIO, "Deleting chunk ", *iter, " failed: ", status);


    }
  } while (keys->size() == max_keys_requested);

  /* Set last chunk number */
  lastChunkNumber.set(chunk_number);
}

void FileIo::Remove(uint16_t timeout)
{
  Truncate(0);
  KineticStatus status = cluster->remove(make_shared<string>(obj_path),
                                         make_shared<string>(), true);
  if (!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND)
    throw kio_exception(EIO, "Could not delete metdata key ", obj_path, ": ", status);
}

void FileIo::Sync(uint16_t timeout)
{
  if (!cluster)
    throw kio_exception(ENXIO, "No cluster set for FileIO object ", obj_path);

  cache.flush(this);
}

void FileIo::Stat(struct stat *buf, uint16_t timeout)
{
  if (!cluster)
    throw kio_exception(ENXIO, "No cluster set for FileIO object ", obj_path);

  lastChunkNumber.verify();
  std::shared_ptr<ClusterChunk> last_chunk = cache.get(this, lastChunkNumber.get(), ClusterChunk::Mode::STANDARD);

  memset(buf, 0, sizeof(struct stat));
  buf->st_blksize = cluster->limits().max_value_size;
  buf->st_blocks = lastChunkNumber.get() + 1;
  buf->st_size = lastChunkNumber.get() * buf->st_blksize + last_chunk->size();
}


void FileIo::Statfs(const char *p, struct statfs *sfs)
{
  if (obj_path.length() && obj_path.compare(p))
    throw kio_exception(EINVAL, "Object concurrently used for both Statfs and FileIO: ", obj_path);

  if (!cluster) {
    cluster = ClusterMap::getInstance().getCluster(utility::extractClusterID(p));
    obj_path = p;
  }

  ClusterSize s = cluster->size();
  if (!s.bytes_total)
    throw kio_exception(EIO, "Could not obtain cluster size values: ", obj_path);

  /* Minimal allocated block size. Set to 4K because that's the
   * maximum accepted value by Linux. */
  sfs->f_frsize = 4096;
  /* Preferred file system block size for I/O requests. This is sometimes
   * evaluated as the actual block size (e.g. by EOS). We set the bsize equal
   * to the frsize to avoid confusion. This approach is also taken by all
   * kernel level file systems. */
  sfs->f_bsize = sfs->f_frsize;
  /* Blocks on FS in units of f_frsize */
  sfs->f_blocks = (fsblkcnt_t) (s.bytes_total / sfs->f_frsize);
  /* Free blocks */
  sfs->f_bavail = (fsblkcnt_t) (s.bytes_free / sfs->f_frsize);
  /* Free blocks available to non root user */
  sfs->f_bfree = sfs->f_bavail;
  /* Total inodes. */
  sfs->f_files = s.bytes_total;
  /* Free inodes */
  sfs->f_ffree = s.bytes_free;
}

struct ftsState
{
  std::unique_ptr<std::vector<string>> keys;
  shared_ptr<string> end_key;
  size_t index;

  ftsState(std::string subtree) : keys(new std::vector<string>({subtree})), index(1)
  {
    end_key = make_shared<string>(subtree + "~");
  }
};

void *FileIo::ftsOpen(std::string subtree)
{
  cluster = ClusterMap::getInstance().getCluster(utility::extractClusterID(subtree));
  return new ftsState(subtree);
}

std::string FileIo::ftsRead(void *fts_handle)
{
  if (!fts_handle) return "";
  ftsState *state = (ftsState *) fts_handle;

  if (state->keys->size() <= state->index) {
    const size_t max_key_requests = 100;
    state->index = 0;
    /* add a space character (lowest ascii printable) to make the range request
       non-including. */
    cluster->range(
        make_shared<string>(state->keys->back() + " "),
        state->end_key,
        max_key_requests, state->keys
    );
  }
  return state->keys->empty() ? "" : state->keys->at(state->index++);
}

int FileIo::ftsClose(void *fts_handle)
{
  ftsState *state = (ftsState *) fts_handle;
  if (state) {
    delete state;
    return 0;
  }
  return -1;
}

FileIo::LastChunkNumber::LastChunkNumber(FileIo &parent) :
    parent(parent), last_chunk_number(0), last_chunk_number_timestamp() { }

FileIo::LastChunkNumber::~LastChunkNumber() { }

int FileIo::LastChunkNumber::get() const
{
  return last_chunk_number;
}

void FileIo::LastChunkNumber::set(int chunk_number)
{
  last_chunk_number = chunk_number;
  last_chunk_number_timestamp = system_clock::now();
}

void FileIo::LastChunkNumber::verify()
{
  /* chunk number verification independent of STANDARD expiration verification
   * in KinetiChunk class. validate last_chunk_number (another client might have
   * created new chunks we know nothing about, or truncated the file. */
  if (std::chrono::duration_cast<std::chrono::milliseconds>(
      system_clock::now() - last_chunk_number_timestamp)
      < ClusterChunk::expiration_time
      )
    return;

  /* Technically, we could start at chunk 0 to catch all cases... but that the
   * file is truncated by another client while opened here is highly unlikely.
   * And for big files this would mean unnecessary GetKeyRange requests for the
   * regular case.  */
  const size_t max_keys_requested = 100;
  std::unique_ptr<std::vector<string>> keys;
  do {
    KineticStatus status = parent.cluster->range(keys ?
                                                 make_shared<const string>(keys->back()) :
                                                 utility::constructChunkKey(parent.chunk_basename, last_chunk_number),
                                                 utility::constructChunkKey(parent.chunk_basename,
                                                                            std::numeric_limits<int>::max()),
                                                 max_keys_requested,
                                                 keys);

    if (!status.ok())
      throw kio_exception(EIO, "KeyRange request unexpectedly failed for chunks with base name: ",
                          parent.chunk_basename, ": ", status);
  } while (keys->size() == max_keys_requested);

  /* Success: get chunk number from last key.*/
  if (keys->size() > 0) {
    std::string key = keys->back();
    std::string number = key.substr(key.find_last_of('_') + 1, key.length());
    set(std::stoll(number));
    return;
  }

  /* No keys found. the file might have been truncated, retry but start the
   * search from chunk 0 this time. */
  if (last_chunk_number > 0) {
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
  if (!status.ok())
    throw kio_exception(ENOENT, "File does not exist: ", parent.obj_path);
}


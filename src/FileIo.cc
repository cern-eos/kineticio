#include "Logging.hh"
#include "FileIo.hh"
#include "ClusterMap.hh"

using std::shared_ptr;
using std::unique_ptr;
using std::string;
using std::make_shared;
using kinetic::KineticStatus;
using kinetic::StatusCode;

using namespace kio;


FileIo::FileIo() :
    cluster(), cache(NULL), lastBlockNumber(*this)
{
}

FileIo::~FileIo()
{
  /* If fileIo object is destroyed without closing properly, throw cache data out the window. */
  if(cache)
  cache->drop(this, true);
}

/* All necessary checks have been done in the 993 line long
 * XrdFstOfsFile::open method before we are called. */
void FileIo::Open(const std::string &p, int flags,
                  mode_t mode, const std::string &opaque, uint16_t timeout)
{
  auto c = ClusterMap::getInstance().getCluster(utility::extractClusterID(p));

  KineticStatus status(StatusCode::CLIENT_INTERNAL_ERROR, "");
  if(flags & SFS_O_CREAT) {
    shared_ptr<const string> version;
    status = c->put(
        make_shared<string>(p),
        make_shared<const string>(),
        make_shared<const string>(),
        false,
        version);

    if (status.ok())
      lastBlockNumber.set(0);
    else if (status.statusCode() == StatusCode::REMOTE_VERSION_MISMATCH)
      throw kio_exception(EEXIST, "File ", p, " already exists (O_CREAT flag set).");
  }
  else {
    shared_ptr<const string> version;
    std::shared_ptr<const string> value;
    status = c->get(
        make_shared<string>(p),
        true,
        version,
        value
    );
    if(status.statusCode() == StatusCode::REMOTE_NOT_FOUND)
      throw kio_exception(ENOENT, "File ", p, " does not exist and cannot be opened without O_CREAT flag.");
  }

  if(!status.ok())
    throw kio_exception(EIO, "Unexpected error opening file ", p, ": ", status);

  /* Setting cluster & path variables. */
  cache = &(ClusterMap::getInstance().getCache());
  cluster = c;
  obj_path = p;
  block_basename = obj_path.substr(obj_path.find_last_of(':') + 1, obj_path.length());
}


void FileIo::Close(uint16_t timeout)
{
  if (!cluster)
    throw kio_exception(ENXIO, "No cluster set for FileIO object ", obj_path);

  cache->flush(this);
  cache->drop(this);
  cluster.reset();
  obj_path.clear();
  block_basename.clear();
}


int64_t FileIo::ReadWrite(long long off, char *buffer,
                          int length, FileIo::rw mode, uint16_t timeout)
{
  if (!cluster)
    throw kio_exception(ENXIO, "No cluster set for FileIO object ", obj_path);

  const size_t block_capacity = cluster->limits().max_value_size;
  size_t length_todo = length;
  off_t off_done = 0;

  while (length_todo) {
    int block_number = static_cast<int>((off + off_done) / block_capacity);
    off_t block_offset = (off + off_done) - block_number * block_capacity;
    size_t block_length = std::min(length_todo, block_capacity - block_offset);

    /* Increase last block number if we write past currently known file size...*/
    DataBlock::Mode cm = DataBlock::Mode::STANDARD;
    if (mode == rw::WRITE && block_number > lastBlockNumber.get()) {
      lastBlockNumber.set(block_number);
      cm = DataBlock::Mode::CREATE;
    }
    
    /* Enable potential prefetch if we are accessing file anywhere but the end. */
    auto prefetch = block_number < lastBlockNumber.get() ? true: false; 
    
    /* Get the data block */
    auto data = cache->get(this, block_number, cm, prefetch);

    if (mode == rw::WRITE) {
      data->write(buffer + off_done, block_offset, block_length);

      /* Flush data in background if writing to block capacity.*/
      if (block_offset + block_length == block_capacity)
        cache->async_flush(this, data);
    }
    else if (mode == rw::READ) {
      data->read(buffer + off_done, block_offset, block_length);

      /* If it looks like we are reading the last block (or past it) */
      if (block_number >= lastBlockNumber.get()) {     
        
        /* First verify that the stored last block number is still up to date. */
        lastBlockNumber.verify();
        if(block_number < lastBlockNumber.get()) 
          continue;     
        
        /* make sure length doesn't indicate that we read past filesize. */
        if (data->size() > block_offset)
          length_todo -= std::min(block_length, data->size() - block_offset);
        break;
      }
    }
    length_todo -= block_length;
    off_done += block_length;
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

  const size_t block_capacity = cluster->limits().max_value_size;
  int block_number = offset / block_capacity;
  int block_offset = offset - block_number * block_capacity;

  if(offset>0){
    /* Step 1) truncate the block containing the offset. */
    cache->get(this, block_number, DataBlock::Mode::STANDARD, false)->truncate(block_offset);

    /* Step 2) Ensure we don't have data past block_number in the cache-> Since
     * truncate isn't super common, go the easy way and just sync+drop the entire
     * cache for this object... this will also sync the just truncated data.  */
    cache->flush(this);
  }
  cache->drop(this, true);

  /* Step 3) Delete all blocks past block_number. When truncating to size 0,
   * (and only then) also delete the first block. */
  std::unique_ptr<std::vector<string>> keys;
  const size_t max_keys_requested = 100;
  do {
    KineticStatus status = cluster->range(
        utility::constructBlockKey(block_basename, offset ? block_number + 1 : 0),
        utility::constructBlockKey(block_basename, std::numeric_limits<int>::max()),
        max_keys_requested, keys);
    if (!status.ok())
      throw kio_exception(EIO, "KeyRange request unexpectedly failed for object ",  obj_path,  ": ", status);

    for (auto iter = keys->begin(); iter != keys->end(); ++iter) {
      status = cluster->remove(make_shared<string>(*iter),
                               make_shared<string>(""), true);
      if (!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND)
        throw kio_exception(EIO, "Deleting block ", *iter, " failed: ", status);
    }
  } while (keys->size() == max_keys_requested);

  /* Set last block number */
  lastBlockNumber.set(block_number);
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

  cache->flush(this);
}

void FileIo::Stat(struct stat *buf, uint16_t timeout)
{
  if (!cluster)
    throw kio_exception(ENXIO, "No cluster set for FileIO object ", obj_path);

  lastBlockNumber.verify();
  std::shared_ptr<DataBlock> last_block = cache->get(this, lastBlockNumber.get(), DataBlock::Mode::STANDARD, false);

  memset(buf, 0, sizeof(struct stat));
  buf->st_blksize = cluster->limits().max_value_size;
  buf->st_blocks = lastBlockNumber.get() + 1;
  buf->st_size = lastBlockNumber.get() * buf->st_blksize + last_block->size();
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

FileIo::LastBlockNumber::LastBlockNumber(FileIo &parent) :
    parent(parent), last_block_number(0), last_block_number_timestamp() { }

FileIo::LastBlockNumber::~LastBlockNumber() { }

int FileIo::LastBlockNumber::get() const
{
  return last_block_number;
}

void FileIo::LastBlockNumber::set(int block_number)
{
  last_block_number = block_number; 
  last_block_number_timestamp = std::chrono::system_clock::now();
}

void FileIo::LastBlockNumber::verify()
{
  using namespace std::chrono;
  /* block number verification independent of  expiration verification
   * in KinetiChunk class. validate last_block_number (another client might have
   * created new blocks we know nothing about, or truncated the file. */
  if (duration_cast<milliseconds>(system_clock::now() - last_block_number_timestamp) < DataBlock::expiration_time)
    return;

  /* Technically, we could start at block 0 to catch all cases... but that the
   * file is truncated by another client while opened here is highly unlikely.
   * And for big files this would mean unnecessary GetKeyRange requests for the
   * regular case.  */
  const size_t max_keys_requested = 100;
  std::unique_ptr<std::vector<string>> keys;
  do {
    KineticStatus status = parent.cluster->range(
        keys ? make_shared<const string>(keys->back()) :
               utility::constructBlockKey(parent.block_basename, last_block_number),
        utility::constructBlockKey(parent.block_basename,std::numeric_limits<int>::max()),
        max_keys_requested,
        keys);

    if (!status.ok())
      throw kio_exception(EIO, "KeyRange request unexpectedly failed for blocks with base name: ",
                          parent.block_basename, ": ", status);
  } while (keys->size() == max_keys_requested);

  /* Success: get block number from last key.*/
  if (keys->size() > 0) {
    std::string key = keys->back();
    std::string number = key.substr(key.find_last_of('_') + 1, key.length());
    set(std::stoll(number));
    return;
  }

  /* No keys found. the file might have been truncated, retry but start the
   * search from block 0 this time. */
  if (last_block_number > 0) {
    last_block_number = 0;
    return verify();
  }

  /* No block keys found. Ensure that the key has not been removed by testing for
     the existence of the metadata key. */
  shared_ptr<const string> version;
  shared_ptr<const string> value;
  auto status = parent.cluster->get(
      make_shared<const string>(parent.obj_path),
      true, version, value);
  if (!status.ok())
    throw kio_exception(ENOENT, "File does not exist: ", parent.obj_path);
}


#include "Logging.hh"
#include "FileIo.hh"
#include "ClusterMap.hh"
#include "KineticIoSingleton.hh"

using std::shared_ptr;
using std::unique_ptr;
using std::string;
using std::make_shared;
using kinetic::KineticStatus;
using kinetic::StatusCode;

using namespace kio;


FileIo::FileIo(const std::string& full_path) :
    mdCluster(), dataCluster(), lastBlockNumber(*this), prefetchOracle(kio().readaheadWindowSize()), opened(false)
{
  path = utility::extractBasePath(full_path);
  auto clusterId = utility::extractClusterID(full_path);
  mdCluster = kio().cmap().getCluster(clusterId, RedundancyType::REPLICATION);
  dataCluster = kio().cmap().getCluster(clusterId, RedundancyType::ERASURE_CODING);
}

FileIo::~FileIo()
{
  /* In case fileIo object is destroyed without having been closed, throw cache data out the window. If
   * object has been closed, cache will have already been dropped. */
  kio().cache().drop(this, true);
}

void FileIo::Open(int flags, mode_t mode, const std::string& opaque, uint16_t timeout)
{
  if (opened) {
    kio_warning("Calling open on already opened file io object: ", path);
  }

  auto mdkey = utility::makeMetadataKey(mdCluster->id(), path);

  KineticStatus status(StatusCode::CLIENT_INTERNAL_ERROR, "");
  if (flags & SFS_O_CREAT) {
    shared_ptr<const string> version;
    status = mdCluster->put(
        mdkey,
        make_shared<const string>(),
        make_shared<const string>(),
        false,
        version);

    if (status.ok()) {
      lastBlockNumber.set(0);
    }
    else if (status.statusCode() == StatusCode::REMOTE_VERSION_MISMATCH) {
      kio_notice("File ", path, " already exists (O_CREAT flag set).");
      throw std::system_error(std::make_error_code(std::errc::file_exists));
    }
  }
  else {
    shared_ptr<const string> version;
    std::shared_ptr<const string> value;
    status = mdCluster->get(
        mdkey,
        true,
        version,
        value
    );
    if (status.statusCode() == StatusCode::REMOTE_NOT_FOUND) {
      kio_notice("File ", path, " does not exist and cannot be opened without O_CREAT flag.");
      throw std::system_error(std::make_error_code(std::errc::no_such_file_or_directory));
    }
  }

  if (!status.ok()) {
    kio_error("Unexpected error opening file ", path, ": ", status);
    throw std::system_error(std::make_error_code(std::errc::io_error));
  }
  opened = true;
}

void FileIo::Close(uint16_t timeout)
{
  if (!opened) {
    kio_error("Calling close on unopened file object for path: ", path);
    throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
  }
  kio().cache().flush(this);
  kio().cache().drop(this);
  opened = false;
}

void FileIo::Sync(uint16_t timeout)
{
  if (!opened) {
    kio_error("Calling sync on unopened file object for path: ", path);
    throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
  }
  kio().cache().flush(this);
}

void do_readahead(std::shared_ptr<kio::DataBlock> data)
{
  char buf[1];
  data->read(buf, 0, 1);
}

void FileIo::scheduleReadahead(int blocknumber)
{
  prefetchOracle.add(blocknumber);

  /* Adjust scheduleReadahead to cache utilization. Full force to 0.75 usage, decreasing until 0.95, then disabled. */
  size_t readahead_length = kio().readaheadWindowSize();
  auto cache_utilization = kio().cache().utilization();

  if (cache_utilization > 0.95) {
    readahead_length = 0;
  }
  else if (cache_utilization > 0.75) {
    readahead_length *= ((1.0 - cache_utilization) / 0.25);
  }

  if (readahead_length) {
    auto prediction = prefetchOracle.predict(readahead_length, PrefetchOracle::PredictionType::CONTINUE);
    for (auto it = prediction.cbegin(); it != prediction.cend(); it++) {
      if (*it < lastBlockNumber.get()) {
        auto data = kio().cache().get(this, *it, DataBlock::Mode::STANDARD);
        auto scheduled = kio().threadpool().try_run(std::bind(do_readahead, data));
        if (scheduled)
          kio_debug("Readahead of data block #", *it);
      }
    }
  }
}

void FileIo::doFlush(std::shared_ptr<kio::DataBlock> data)
{
  if (data->dirty()) {
    try {
      data->flush();
    }
    catch (const std::exception& e) {
      kio_warning("Exception ocurred in background flush: ", e.what());
      std::lock_guard<std::mutex> lock(exception_mutex);
      exceptions.push(e);
    }
  }
}

void FileIo::scheduleFlush(std::shared_ptr<kio::DataBlock> data)
{
  kio().threadpool().run(std::bind(&FileIo::doFlush, this, data));
}


int64_t FileIo::ReadWrite(long long off, char* buffer,
                          int length, FileIo::rw mode, uint16_t timeout)
{
  {
    std::lock_guard<std::mutex> lock(exception_mutex);
    if (!exceptions.empty()) {
      auto e = exceptions.front();
      exceptions.pop();
      kio_warning("Re-throwing exception caught in previous async flush operation: ", e.what());
      throw e;
    }
  }

  const size_t block_capacity = dataCluster->limits().max_value_size;
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

    auto data = kio().cache().get(this, block_number, cm);
    scheduleReadahead(block_number);

    if (mode == rw::WRITE) {
      data->write(buffer + off_done, block_offset, block_length);

      /* Flush data in background if writing to block capacity.*/
      if (block_offset + block_length == block_capacity) {
        scheduleFlush(data);
      }
    }
    else if (mode == rw::READ) {
      data->read(buffer + off_done, block_offset, block_length);

      /* If it looks like we are reading the last block (or past it) */
      if (block_number >= lastBlockNumber.get()) {

        /* First verify that the stored last block number is still up to date. */
        lastBlockNumber.verify();
        if (block_number < lastBlockNumber.get()) {
          continue;
        }

        /* make sure length doesn't indicate that we read past filesize. */
        if (data->size() > block_offset) {
          length_todo -= std::min(block_length, data->size() - block_offset);
        }
        break;
      }
    }
    length_todo -= block_length;
    off_done += block_length;
  }

  return length - length_todo;
}

int64_t FileIo::Read(long long offset, char* buffer, int length,
                     uint16_t timeout)
{
  if (!opened) {
    kio_error("Read operation not permitted on non-opened object.");
    throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
  }

  return ReadWrite(offset, buffer, length, FileIo::rw::READ, timeout);
}

int64_t FileIo::Write(long long offset, const char* buffer,
                      int length, uint16_t timeout)
{
  if (!opened) {
    kio_error("Write operation not permitted on non-opened object.");
    throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
  }

  return ReadWrite(offset, const_cast<char*>(buffer), length,
                   FileIo::rw::WRITE, timeout);
}

void FileIo::Truncate(long long offset, uint16_t timeout)
{
  if (!opened) {
    kio_error("Truncate operation not permitted on non-opened object.");
    throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
  }

  const size_t block_capacity = dataCluster->limits().max_value_size;
  int block_number = offset / block_capacity;
  int block_offset = offset - block_number * block_capacity;

  if (offset > 0) {
    /* Step 1) truncate the block containing the offset. */
    kio().cache().get(this, block_number, DataBlock::Mode::STANDARD)->truncate(block_offset);

    /* Step 2) Ensure we don't have data past block_number in the kio().cache() Since
     * truncate isn't super common, go the easy way and just sync+drop the entire
     * cache for this object... this will also sync the just truncated data.  */
    kio().cache().flush(this);
  }
  kio().cache().drop(this, true);

  /* Step 3) Delete all blocks past block_number. When truncating to size 0,
   * (and only then) also delete the first block. */
  std::unique_ptr<std::vector<string>> keys;
  const size_t max_keys_requested = 100;
  do {
    KineticStatus status = dataCluster->range(
        utility::makeDataKey(dataCluster->id(), path, offset ? block_number + 1 : 0),
        utility::makeDataKey(dataCluster->id(), path, std::numeric_limits<int>::max()),
        max_keys_requested, keys);
    if (!status.ok()) {
      kio_error("KeyRange request unexpectedly failed for path ", path, ": ", status);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    };

    for (auto iter = keys->begin(); iter != keys->end(); ++iter) {
      status = dataCluster->remove(make_shared<string>(*iter),
                                   make_shared<string>(), true);
      if (!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND) {
        kio_error("Deleting block ", *iter, " failed: ", status);
        throw std::system_error(std::make_error_code(std::errc::io_error));
      }
    }
  } while (keys->size() == max_keys_requested);

  /* Set last block number */
  lastBlockNumber.set(block_number);
}

void FileIo::Remove(uint16_t timeout)
{
  /* We allow removing unopened files... */
  if (!opened) {
    Open(0);
  }

  auto attributes = attrList();

  KineticStatus status(StatusCode::CLIENT_INTERNAL_ERROR, "");
  for (auto iter = attributes.begin(); iter != attributes.end(); ++iter) {
    status = mdCluster->remove(utility::makeAttributeKey(mdCluster->id(), this->path, *iter),
                               std::make_shared<std::string>(),
                               true);
    if (!status.ok()) {
      kio_error("Deleting attribute ", *iter, " failed: ", status);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    }
  }

  Truncate(0);
  status = mdCluster->remove(utility::makeMetadataKey(mdCluster->id(), path), make_shared<string>(), true);
  if (!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND) {
    kio_error("Could not delete metdata key for path", path, ": ", status);
    throw std::system_error(std::make_error_code(std::errc::io_error));
  }
}

void FileIo::Stat(struct stat* buf, uint16_t timeout)
{
  if (!opened) {
    kio_error("Stat operation not permitted on non-opened object.");
    throw std::system_error(std::make_error_code(std::errc::operation_not_permitted));
  }

  lastBlockNumber.verify();
  auto last_block = kio().cache().get(this, lastBlockNumber.get(), DataBlock::Mode::STANDARD);

  memset(buf, 0, sizeof(struct stat));
  buf->st_blksize = dataCluster->limits().max_value_size;
  buf->st_blocks = lastBlockNumber.get() + 1;
  buf->st_size = lastBlockNumber.get() * buf->st_blksize + last_block->size();
}

enum class RequestType {
  STANDARD, READ_OPS, READ_BW, WRITE_OPS, WRITE_BW, MAX_BW
};

RequestType getRequestType(const char* name)
{
  if (strcmp(name, "sys.iostats.max-bw") == 0) {
    return RequestType::MAX_BW;
  }
  if (strcmp(name, "sys.iostats.read-bw") == 0) {
    return RequestType::READ_BW;
  }
  if (strcmp(name, "sys.iostats.read-ops") == 0) {
    return RequestType::READ_OPS;
  }
  if (strcmp(name, "sys.iostats.write-bw") == 0) {
    return RequestType::WRITE_BW;
  }
  if (strcmp(name, "sys.iostats.write-ops") == 0) {
    return RequestType::WRITE_OPS;
  }
  return RequestType::STANDARD;
}

std::string FileIo::attrGet(std::string name)
{
  auto type = getRequestType(name.c_str());

  if (type == RequestType::STANDARD) {
    std::shared_ptr<const string> value;
    std::shared_ptr<const string> version;
    auto status = mdCluster->get(
        utility::makeAttributeKey(mdCluster->id(), path, name),
        false, version, value);

    /* Requested attribute doesn't exist or there was connection problem. */
    if (status.statusCode() == kinetic::StatusCode::REMOTE_NOT_FOUND) {
      kio_debug("Requested attribute ", name, " does not exist");
      throw std::system_error(std::make_error_code(std::errc::no_such_file_or_directory));
    }
    if (!status.ok()) {
      kio_error("Error attempting to access attribute ", name, ": ", status);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    }
    return *value;
  }

  auto stats = mdCluster->iostats();
  double MB = 1024 * 1024;
  double result = 0;
  switch (type) {
    case RequestType::MAX_BW:
      result = stats.number_drives * 50 * MB;
      break;
    case RequestType::READ_BW:
      result = stats.read_bytes / MB;
      break;
    case RequestType::READ_OPS:
      result = stats.read_ops;
      break;
    case RequestType::WRITE_BW:
      result = stats.write_bytes / MB;
      break;
    case RequestType::WRITE_OPS:
      result = stats.write_ops;
      break;
    default:
      kio_error("Invalid request type.");
      throw std::system_error(std::make_error_code(std::errc::invalid_argument));
  }
  return utility::Convert::toString(result);
}

void FileIo::attrSet(std::string name, std::string value)
{
  auto empty = std::make_shared<const string>();
  auto status = mdCluster->put(
      utility::makeAttributeKey(mdCluster->id(), path, name),
      empty,
      std::make_shared<const string>(value),
      true,
      empty
  );

  if (!status.ok()) {
    kio_error("Failed setting attribute ", name, " due to: ", status);
    throw std::system_error(std::make_error_code(std::errc::io_error));
  }
}

void FileIo::attrDelete(std::string name)
{
  auto empty = std::make_shared<const string>();
  auto status = mdCluster->remove(
      utility::makeAttributeKey(mdCluster->id(), path, name),
      empty,
      true
  );
  if (!status.ok()) {
    kio_error("Failed getting attribute ", name, " due to: ", status);
    throw std::system_error(std::make_error_code(std::errc::io_error));
  }
}

std::vector<std::string> FileIo::attrList()
{
  std::unique_ptr<std::vector<string>> keys;
  std::vector<std::string> names;

  auto start = utility::makeAttributeKey(mdCluster->id(), path, " ");
  auto end = utility::makeAttributeKey(mdCluster->id(), path, "~");
  const size_t max_keys_requested = 100;

  do {
    auto status = mdCluster->range(start, end, max_keys_requested, keys);
    if (!status.ok()) {
      kio_error("KeyRange request unexpectedly failed for path ", path, ": ", status);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    }
    for (auto it = keys->cbegin(); it != keys->cend(); it++) {
      names.push_back(utility::extractAttributeName(*it));
    }
    if (keys->size()) {
      start = std::make_shared<const string>(keys->back());
    }
  } while (keys->size() == max_keys_requested);
  return names;
}

void FileIo::Statfs(struct statfs* sfs)
{
  ClusterSize s = dataCluster->size();
  if (!s.bytes_total) {
    kio_error("Could not obtain cluster size values");
    throw std::system_error(std::make_error_code(std::errc::io_error));
  }

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

std::vector<std::string> FileIo::ListFiles(std::string subtree, size_t max)
{
  /* Verify that the path is contained in the supplied subtree */
  if (subtree.find(path) == std::string::npos) {
    kio_error("Illegal argument ", subtree, " supplied for fileio object with path ", path);
    throw std::system_error(std::make_error_code(std::errc::invalid_argument));
  }

  std::unique_ptr<std::vector<string>> keys;
  std::vector<std::string> names;
  auto subtree_base = utility::extractBasePath(subtree);
  auto start = utility::makeMetadataKey(dataCluster->id(), subtree_base);
  auto end = utility::makeMetadataKey(dataCluster->id(), subtree_base + "~");

  do {
    auto status = dataCluster->range(start, end, 100 < max ? 100 : max, keys);
    if (!status.ok()) {
      kio_error("KeyRange request unexpectedly failed for path ", path, ": ", status);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    }

    for (auto it = keys->cbegin(); it != keys->cend(); it++) {
      names.push_back(utility::metadataToPath(*it));
    }
    if (keys->size()) {
      start = std::make_shared<const string>(keys->back() + " ");
    }
  } while (keys->size() == 100 && max > 100);
  return names;
}


FileIo::LastBlockNumber::LastBlockNumber(FileIo& parent) :
    parent(parent), last_block_number(0), last_block_number_timestamp()
{ }

FileIo::LastBlockNumber::~LastBlockNumber()
{ }

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
  if (duration_cast<milliseconds>(system_clock::now() - last_block_number_timestamp) < DataBlock::expiration_time) {
    return;
  }

  /* Technically, we could start at block 0 to catch all cases... but that the
   * file is truncated by another client while opened here is highly unlikely.
   * And for big files this would mean unnecessary GetKeyRange requests for the
   * regular case.  */
  const size_t max_keys_requested = 100;
  std::unique_ptr<std::vector<string>> keys;
  do {
    KineticStatus status = parent.dataCluster->range(
        keys ? make_shared<const string>(keys->back()) :
        utility::makeDataKey(parent.dataCluster->id(), parent.path, last_block_number),
        utility::makeDataKey(parent.dataCluster->id(), parent.path, std::numeric_limits<int>::max()),
        max_keys_requested,
        keys);

    if (!status.ok()) {
      kio_error("KeyRange request unexpectedly failed for blocks of path: ", parent.path, ": ", status);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    }
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
  auto status = parent.dataCluster->get(
      make_shared<const string>(parent.path),
      true, version, value);
  if (!status.ok()) {
    kio_warning("File has been removed: ", parent.path);
    throw std::system_error(std::make_error_code(std::errc::no_such_file_or_directory));
  }
}

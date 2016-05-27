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


FileIo::FileIo(const std::string& url) :
    cluster(), prefetchOracle(kio().readaheadWindowSize()), opened(false)
{
  if (url.compare(0, strlen("kinetic://"), "kinetic://") != 0) {
    kio_error("Invalid url supplied. Required format: kinetic://clusterId/path, supplied: ", url);
    throw std::system_error(std::make_error_code(std::errc::invalid_argument));
  }

  path = utility::urlToPath(url);
  cluster = kio().cmap().getCluster(
      utility::urlToClusterId(url)
  );
}

FileIo::~FileIo()
{
  /* In case fileIo object is destroyed without having been closed, throw cache data out the window. If
   * object has been closed, cache will have already been dropped. */
  kio().cache().drop(this, true);
}

void FileIo::Open(int flags, mode_t mode, const std::string& opaque, uint16_t timeout)
{
  auto mdkey = utility::makeMetadataKey(cluster->id(), path);

  KineticStatus status(StatusCode::CLIENT_INTERNAL_ERROR, "");
  if (flags & SFS_O_CREAT) {
    shared_ptr<const string> version;
    status = cluster->put(
        mdkey,
        make_shared<const string>(),
        make_shared<const string>(),
        version,
        KeyType::Metadata);

    if (status.ok()) {
      eof_blocknumber = size_hint = 0;
      eof_verification_time = std::chrono::system_clock::now();
    }
    else if (status.statusCode() == StatusCode::REMOTE_VERSION_MISMATCH) {
      kio_debug("File ", path, " already exists (O_CREAT flag set).");
      throw std::system_error(std::make_error_code(std::errc::file_exists));
    }
  }
  else {
    shared_ptr<const string> version;
    status = cluster->get(
        mdkey,
        version,
        KeyType::Metadata
    );

    if (status.ok()) {
      try {
        /* Size hint might be stored in attribute. */
        eof_blocknumber = size_hint = utility::Convert::toInt(attrGet("sys.kinetic.size_hint"));
      } catch (const std::system_error& e) {
        /* no size hint available. not a problem */
        eof_blocknumber = size_hint = 0;
      }
      eof_verification_time = std::chrono::system_clock::time_point();
    }
    else if (status.statusCode() == StatusCode::REMOTE_NOT_FOUND) {
      kio_debug("File ", path, " does not exist and cannot be opened without O_CREAT flag.");
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
  /* If we would need more than one cluster::range request in the stat operation to find the correct fill size
   * create or update the size_hint attribute.
   * This conditional size_hint attribut ewill minimize IO for stat but at the same time avoid unecessary attribute
   * IO on every close. */
  size_t hint_difference = std::abs(eof_blocknumber - size_hint);
  size_t max_request_size = cluster->limits(KeyType::Data).max_range_elements;
  if (hint_difference > max_request_size) {
    try { attrSet("sys.kinetic.size_hint", utility::Convert::toString(eof_blocknumber)); } catch (...) { }
  }

  eof_blocknumber = size_hint = 0;
  opened = false;
  kio().cache().flush(this);
  kio().cache().drop(this);
}

void FileIo::Sync(uint16_t timeout)
{
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

  /* Adjust to cache utilization. Full force to 0.75 usage, decreasing until 0.95, then disabled. */
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
      if (*it < eof_blocknumber) {
        auto data = kio().cache().getDataKey(this, *it, DataBlock::Mode::STANDARD);
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
    catch (const std::system_error& e) {
      kio_warning("Exception ocurred in background flush of data block ", data->getIdentity(), ": ", e.what());
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

  const size_t block_capacity = cluster->limits(KeyType::Data).max_value_size;
  size_t length_todo = static_cast<size_t>(length);
  size_t off_done = 0;

  while (length_todo) {
    int block_number = static_cast<int>((off + off_done) / block_capacity);
    size_t block_offset = (off + off_done) - block_number * block_capacity;
    size_t block_length = std::min(length_todo, block_capacity - block_offset);

    /* Increase last block number if we write past currently known file size...*/
    DataBlock::Mode cm = DataBlock::Mode::STANDARD;
    if (mode == rw::WRITE && block_number > eof_blocknumber) {
      eof_blocknumber = block_number;
      cm = DataBlock::Mode::CREATE;
    }

    auto data = kio().cache().getDataKey(this, block_number, cm);
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
      if (block_number >= eof_blocknumber) {

        /* First verify that the stored last block number is still up to date. */
        verify_eof();
        if (block_number < eof_blocknumber) {
          continue;
        }

        /* make sure length doesn't indicate that we read past file size. */
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

  const size_t block_capacity = cluster->limits(KeyType::Data).max_value_size;
  int block_number = static_cast<int>(offset / block_capacity);
  size_t block_offset = offset - block_number * block_capacity;

  if (offset > 0) {
    /* Step 1) truncate the block containing the offset. */
    kio().cache().getDataKey(this, block_number, DataBlock::Mode::STANDARD)->truncate(block_offset);

    /* Step 2) Ensure we don't have data past block_number in the kio().cache() Since
     * truncate isn't super common, go the easy way and just sync+drop the entire
     * cache for this object... this will also sync the just truncated data.  */
    kio().cache().flush(this);
  }
  kio().cache().drop(this, true);

  /* Step 3) Delete all blocks past block_number. When truncating to size 0,
   * (and only then) also delete the first block. */
  std::unique_ptr<std::vector<string>> keys;
  do {
    KineticStatus status = cluster->range(
        utility::makeDataKey(cluster->id(), path, offset ? block_number + 1 : 0),
        utility::makeDataKey(cluster->id(), path, std::numeric_limits<int>::max()),
        keys, KeyType::Data);
    if (!status.ok()) {
      kio_error("KeyRange request unexpectedly failed for path ", path, ": ", status);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    };

    for (auto iter = keys->begin(); iter != keys->end(); ++iter) {
      status = cluster->remove(make_shared<string>(*iter), KeyType::Data);
      if (!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND) {
        kio_error("Deleting block ", *iter, " failed: ", status);
        throw std::system_error(std::make_error_code(std::errc::io_error));
      }
    }
  } while (keys->size() == cluster->limits(KeyType::Data).max_range_elements);

  /* Set last block number */
  eof_blocknumber = block_number;
}

void FileIo::Remove(uint16_t timeout)
{
  /* We allow removing unopened files... */
  if (!opened) {
    Open(0);
  }

  auto attributes = attrList();

  KineticStatus status(StatusCode::CLIENT_INTERNAL_ERROR, "");
  for (auto iter = attributes.cbegin(); iter != attributes.cend(); ++iter) {
    status = cluster->remove(utility::makeAttributeKey(cluster->id(), path, *iter), KeyType::Metadata);
    if (!status.ok()) {
      kio_error("Deleting attribute ", *iter, " failed: ", status);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    }
  }

  Truncate(0);
  status = cluster->remove(utility::makeMetadataKey(cluster->id(), path), KeyType::Metadata);
  if (!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND) {
    kio_error("Could not delete metdata key for path", path, ": ", status);
    throw std::system_error(std::make_error_code(std::errc::io_error));
  }
}


int FileIo::get_eof_backend()
{
  /* Most likely the currently stored eof block number is correct... so let's start there. */
  std::unique_ptr<std::vector<string>> keys;
  auto start_key = utility::makeDataKey(cluster->id(), path, eof_blocknumber);
  auto end_key = utility::makeDataKey(cluster->id(), path, std::numeric_limits<int>::max());

  KineticStatus status = cluster->range(start_key, end_key, keys, KeyType::Data);

  /* Found no keys... backend file might have been truncated by another client or writes might
   * not have been flushed yet. */
  if (status.ok() && !keys->size()) {
    keys->push_back(*utility::makeDataKey(cluster->id(), path, 0));
    do {
      start_key = make_shared<const string>(keys->back());
      keys->pop_back();
      status = cluster->range(start_key, end_key, keys, KeyType::Data);
    } while (status.ok() && keys->size() == cluster->limits(KeyType::Data).max_range_elements);
  }

  if (!status.ok()) {
    kio_error("KeyRange request unexpectedly failed for blocks of path: ", path, ": ", status);
    throw std::system_error(std::make_error_code(std::errc::io_error));
  }

  /* Success: get block number from last key.*/
  if (keys->size() > 0) {
    std::string key = keys->back();
    std::string number = key.substr(key.find_last_of('_') + 1, key.length());
    return std::stoi(number);
  }

  /* No block keys found. Ensure that the key has not been removed by testing for
     the existence of the metadata key. */
  shared_ptr<const string> version;
  auto mdkey = utility::makeMetadataKey(cluster->id(), path);
  if (cluster->get(mdkey, version, KeyType::Metadata).ok()) {
    return 0;
  }
  kio_warning("File does not exist: ", path);
  throw std::system_error(std::make_error_code(std::errc::no_such_file_or_directory));
}

void FileIo::verify_eof()
{
  using namespace std::chrono;
  if (duration_cast<milliseconds>(system_clock::now() - eof_verification_time) > DataBlock::expiration_time) {

    eof_verification_time = std::chrono::system_clock::now();

    int backend = get_eof_backend();
    if (backend >= eof_blocknumber) {
      eof_blocknumber = backend;
      return;
    }
    auto last_block = kio().cache().getDataKey(this, eof_blocknumber, DataBlock::Mode::STANDARD);

    /* un-flushed writes mean that eof_blocknumber is correct */
    if (last_block->dirty()) {
      return;
    }
    /* No un-flushed changes... re-get the block number from backend to ensure that there was no race condition */
    eof_blocknumber = get_eof_backend();
  }
}


void FileIo::Stat(struct stat* buf, uint16_t timeout)
{
  /* We allow statting unopened files... */
  if (!opened) {
    Open(0);
  }

  verify_eof();
  auto last_block = kio().cache().getDataKey(this, eof_blocknumber, DataBlock::Mode::STANDARD);

  memset(buf, 0, sizeof(struct stat));
  buf->st_blksize = cluster->limits(KeyType::Data).max_value_size;
  buf->st_blocks = eof_blocknumber + 1;
  buf->st_size = eof_blocknumber * buf->st_blksize + last_block->size();
}

std::string FileIo::attrGet(std::string name)
{
  /* Client may be requesting io stats instead of normal attributes */
  if (name == "sys.iostats") {
    auto stats = cluster->stats();
    using namespace std::chrono;
    double time = duration_cast<seconds>(stats.io_end - stats.io_start).count();
    double MB = 1024 * 1024;

    auto stringstats = utility::Convert::toString(
        "read-mb-total=", stats.read_bytes_total / MB,
        ",read-ops-total=", stats.read_ops_total,
        ",write-mb-total=", stats.write_bytes_total / MB,
        ",write-ops-total=", stats.write_ops_total,
        ",read-mb-second=", (stats.read_bytes_period / time) / MB,
        ",read-ops-second=", stats.read_ops_period / time,
        ",write-mb-second=", (stats.write_bytes_period / time) / MB,
        ",write-ops-second=", stats.write_ops_period / time
    );
    kio_debug(stringstats);
    return stringstats;
  }

  std::shared_ptr<const string> value;
  std::shared_ptr<const string> version;
  auto status = cluster->get(
      utility::makeAttributeKey(cluster->id(), path, name),
      version, value, KeyType::Metadata);
  if (status.ok()) {
    return *value;
  }

  /* Requested attribute doesn't exist or there was connection problem. */
  if (status.statusCode() == kinetic::StatusCode::REMOTE_NOT_FOUND) {
    kio_debug("Requested attribute ", name, " does not exist");
    throw std::system_error(std::make_error_code(std::errc::no_such_file_or_directory));
  }

  kio_error("Error attempting to access attribute ", name, ": ", status);
  throw std::system_error(std::make_error_code(std::errc::io_error));
}

void FileIo::attrSet(std::string name, std::string value)
{
  auto empty = std::make_shared<const string>();
  auto status = cluster->put(
      utility::makeAttributeKey(cluster->id(), path, name),
      std::make_shared<const string>(value),
      empty,
      KeyType::Metadata
  );

  if (!status.ok()) {
    kio_error("Failed setting attribute ", name, " due to: ", status);
    throw std::system_error(std::make_error_code(std::errc::io_error));
  }
}

void FileIo::attrDelete(std::string name)
{
  auto empty = std::make_shared<const string>();
  auto status = cluster->remove(
      utility::makeAttributeKey(cluster->id(), path, name), KeyType::Metadata
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

  auto start = utility::makeAttributeKey(cluster->id(), path, " ");
  auto end = utility::makeAttributeKey(cluster->id(), path, "~");

  do {
    auto status = cluster->range(start, end, keys, KeyType::Metadata);
    if (!status.ok()) {
      kio_error("KeyRange request unexpectedly failed for path ", path, ": ", status);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    }
    for (auto it = keys->cbegin(); it != keys->cend(); it++) {
      names.push_back(utility::extractAttributeName(cluster->id(), path, *it));
    }
    if (keys->size()) {
      start = std::make_shared<const string>(keys->back());
    }
  } while (keys->size() == cluster->limits(KeyType::Metadata).max_range_elements);
  return names;
}

void FileIo::Statfs(struct statfs* sfs)
{
  auto s = cluster->stats();
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
  /* Total inodes */
  sfs->f_files = (size_t) (s.bytes_total / sfs->f_frsize);
  /* Free inodes */
  sfs->f_ffree = (size_t) (s.bytes_free / sfs->f_frsize);
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
  auto subtree_base = utility::urlToPath(subtree);
  auto start = utility::makeMetadataKey(cluster->id(), subtree_base);
  auto end = utility::makeMetadataKey(cluster->id(), subtree_base + "~");

  do {
    auto status = cluster->range(start, end, keys, KeyType::Metadata);
    if (!status.ok()) {
      kio_error("KeyRange request unexpectedly failed for path ", path, ": ", status);
      throw std::system_error(std::make_error_code(std::errc::io_error));
    }

    for (auto it = keys->cbegin(); it != keys->cend() && names.size() < max; it++) {
      names.push_back(utility::metadataToUrl(*it));
    }
    if (keys->size()) {
      start = std::make_shared<const string>(keys->back() + " ");
    }
  } while (names.size() < max && keys->size() == cluster->limits(KeyType::Metadata).max_range_elements);
  return names;
}

#include "DataBlock.hh"
#include "Utility.hh"
#include "Logging.hh"

using std::unique_ptr;
using std::shared_ptr;
using std::make_shared;
using std::string;
using std::chrono::system_clock;
using kinetic::KineticStatus;
using kinetic::StatusCode;
using namespace kio;

const std::chrono::milliseconds DataBlock::expiration_time(1000);


DataBlock::DataBlock(std::shared_ptr<ClusterInterface> c,
                           const std::shared_ptr<const std::string> k, Mode m) :
    mode(m), cluster(c), key(k), version(), value(make_shared<string>()),
    timestamp(), updates(), mutex()
{
  if (!cluster) throw std::invalid_argument("no cluster supplied");
}

DataBlock::~DataBlock()
{
  // take the mutex in order to prevent object deconsturction while flush
  // operation is executed by non-owning thread.
  std::lock_guard<std::mutex> lock(mutex);
}

std::string DataBlock::getIdentity()
{
  return cluster->id() + *key;
}

bool DataBlock::validateVersion()
{
  /* See if check is unnecessary based on expiration. */
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;
  if (duration_cast<milliseconds>(system_clock::now() - timestamp) < expiration_time)
    return true;

  /*If we are reading for the first time from a block opened in STANDARD mode,
    skip version validation and jump straight to the get operation. */
  if (!version && mode == Mode::STANDARD)
    return false;

  /* Check remote version & compare it to in-memory version. */
  shared_ptr<const string> remote_version;
  shared_ptr<const string> remote_value;
  KineticStatus status = cluster->get(key, true, remote_version, remote_value);

  /*If no version is set, the entry has never been flushed. In this case,
    not finding an entry with that key in the cluster is expected. */
  if ((!version && status.statusCode() == StatusCode::REMOTE_NOT_FOUND) ||
      (status.ok() && remote_version && version && *version == *remote_version)
      ) {
    /* In memory version equals remote version. Remember the time. */
    timestamp = system_clock::now();
    return true;
  }
  return false;
}

void DataBlock::getRemoteValue()
{
  auto remote_value = make_shared<const string>("");
  auto status = cluster->get(key, false, version, remote_value);

  if (!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND)
    throw kio_exception(EIO, "Attempting to read key '",  *key,  "' from cluster returned error ", status);

  /* We read in the current value from the drive. Remember the time. */
  timestamp = system_clock::now();

  /* If remote is not available, reset version. */
  if (status.statusCode() == StatusCode::REMOTE_NOT_FOUND)
    version.reset();

  /* Merge all updates done on the local data copy (data) into the freshly
     read-in data copy. */
  auto merged_value = make_shared<string>(*remote_value);
  if(!updates.empty())
    merged_value->resize(std::max(remote_value->size(), value->size()));

  for (auto iter = updates.begin(); iter != updates.end(); ++iter) {
    auto update = *iter;
    if (update.second)
      merged_value->replace(update.first, update.second, *value, update.first, update.second);
    else
      merged_value->resize(update.first);
  }
  value = merged_value;
}

void DataBlock::read(char *const buffer, off_t offset, size_t length)
{
  std::lock_guard<std::mutex> lock(mutex);

  if (buffer == NULL) throw std::invalid_argument("null buffer supplied");
  if (offset < 0) throw std::invalid_argument("negative offset");
  if (offset + length > cluster->limits().max_value_size)
    throw std::invalid_argument("attempting to read past cluster limits");

  /*Ensure data is not too stale to read.*/
  if (!validateVersion())
    getRemoteValue();

  /* return 0s if client reads non-existing data (e.g. file with holes) */
  if (offset + length > value->size())
    memset(buffer, 0, length);

  if (value->size() > (size_t) offset) {
    size_t copy_length = std::min(length, (size_t) (value->size() - offset));
    value->copy(buffer, copy_length, offset);
  }
}

void DataBlock::write(const char *const buffer, off_t offset, size_t length)
{
  std::lock_guard<std::mutex> lock(mutex);

  if (buffer == NULL) throw std::invalid_argument("null buffer supplied");
  if (offset < 0) throw std::invalid_argument("negative offset");
  if (offset + length > cluster->limits().max_value_size)
    throw std::invalid_argument("attempting to write past cluster limits");

  /* Set new entry size. */
  value->resize(std::max((size_t) offset + length, value->size()));

  /* Copy data and remember write access. */
  value->replace(offset, length, buffer, length);
  updates.push_back(std::pair<off_t, size_t>(offset, length));
}

void DataBlock::truncate(off_t offset)
{
  std::lock_guard<std::mutex> lock(mutex);

  if (offset < 0) throw std::invalid_argument("negative offset");
  if (offset > cluster->limits().max_value_size)
    throw std::invalid_argument("attempting to truncate past cluster limits");

  value->resize(offset);
  updates.push_back(std::pair<off_t, size_t>(offset, 0));
}

void DataBlock::flush()
{
  std::lock_guard<std::mutex> lock(mutex);

  auto status = cluster->put(key, version, value, false, version);
  while (status.statusCode() == StatusCode::REMOTE_VERSION_MISMATCH) {
    getRemoteValue();
    status = cluster->put(key, version, value, false, version);
  }

  if (!status.ok())
    throw kio_exception(EIO, "Attempting to write key '",  *key, "' from cluster returned error ", status);

  /* Success... we can forget about in-memory changes and set timestamp
     to current time. */
  updates.clear();
  timestamp = system_clock::now();
}

bool DataBlock::dirty() const
{
  std::lock_guard<std::mutex> lock(mutex);
  if (!updates.empty())
    return true;

  /* If we opened in create mode, we assume this key doesn't exist yet, it
     is dirty even if we have written nothing to it. If we opened in STANDARD
     mode we assume it does already exist... we just haven't used it. */
  if (!version && mode == Mode::CREATE)
    return true;
  return false;
}

size_t DataBlock::capacity() const
{
  return cluster->limits().max_value_size;
}

size_t DataBlock::size()
{
  std::lock_guard<std::mutex> lock(mutex);

  /* Ensure size is not too stale. */
  if (!validateVersion())
    getRemoteValue();

  return value->size();
}
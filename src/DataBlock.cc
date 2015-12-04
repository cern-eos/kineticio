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


DataBlock::DataBlock(std::shared_ptr<ClusterInterface> c, const std::shared_ptr<const std::string> k, Mode m) :
    mode(m), cluster(c), key(k), version(), remote_value(), local_value(), value_size(0), updates(),
    timestamp(), mutex()
{
  if (!cluster) throw std::invalid_argument("no cluster supplied");
}

DataBlock::~DataBlock()
{
  // take the mutex in order to prevent object deconsturction while flush
  // operation is executed by non-owning thread.
  std::lock_guard<std::mutex> lock(mutex);
}

void DataBlock::reassign(std::shared_ptr<ClusterInterface> c, std::shared_ptr<const std::string> k, Mode m)
{
  if (!cluster) throw std::invalid_argument("no cluster supplied");
 
  key = k;
  mode = m;
  cluster = c; 
  value_size = 0;
  version.reset();
  updates.clear();
  timestamp = system_clock::time_point();
  if(local_value) 
    local_value->assign(capacity(),'0');
}

std::string DataBlock::getIdentity()
{
  return *key + cluster->instanceId();
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
  shared_ptr<const string> empty;
  KineticStatus status = cluster->get(key, true, remote_version, empty);

  /*If no version is set, the entry has never been flushed. In this case,
    not finding an entry with that key in the cluster is expected. */
  if ((!version && status.statusCode() == StatusCode::REMOTE_NOT_FOUND) ||
      (status.ok() && remote_version && version && *version == *remote_version)) 
  {
    /* In memory version equals remote version. Remember the time. */
    timestamp = system_clock::now();
    return true;
  }
  return false;
}

/* This function is written a lot more complex than it should be at first glance. It all is 
 * to avoid / minimize memory allocations and copies as much as possible */
void DataBlock::getRemoteValue()
{
  auto status = cluster->get(key, false, version, remote_value);
  
  if (!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND)
      throw kio_exception(EIO, "Attempting to read key '",  *key,  "' from cluster returned error ", status);

  /* If remote is not available, reset version. */
  if (status.statusCode() == StatusCode::REMOTE_NOT_FOUND) {
    version.reset();
  }
  /* set value_size to reflect remote_value */
  else {
    value_size = remote_value ? remote_value->size() : 0;
  }

  /* We read in the value from the drive. Remember the time. */
  timestamp = system_clock::now();

  /* If there are no local updates, there is no need to do any more work... just ensure that the 
     local_value variable is empty so that all reads will be served from the remote_value */
  if(updates.empty()) {
    local_value.reset();
    return;
  }
  
  /* If we have local updates but no remote value, the local value can be left alone */
  if(!remote_value)
    return;
        
  /* Due to getting a <const string> returned from the cluster, we will have to create yet another string 
   * variable to merge the local changes. */
  auto merged_value = make_shared<string>(*remote_value);

  /* Merge all updates done on the local data copy (value) into the freshly read-in data copy. */
  if(merged_value->size() < capacity())
    merged_value->resize(capacity());

  for (auto iter = updates.begin(); iter != updates.end(); ++iter) {
    auto update = *iter;
    if(!update.second){
      value_size = update.first;
    }
    else{
      value_size = std::max(update.first + update.second, value_size); 
      merged_value->replace(update.first, update.second, *local_value, update.first, update.second);  
    }
  }
  remote_value.reset();
  local_value = std::move(merged_value);
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
  if (offset + length > value_size)
    memset(buffer, 0, length);

  if (value_size > (size_t) offset) {
    size_t copy_length = std::min(length, (size_t) (value_size - offset));
    if(local_value)
      local_value->copy(buffer, copy_length, offset);
    else
      remote_value->copy(buffer, copy_length, offset);
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
  value_size = std::max((size_t) offset + length, value_size);
  
  /* Ensure that the value string exists and is big enough to store the write request. If necessary,
   * we will allocate straight to capacity size to prevent multiple resize operations making
   * a mess of heap allocation (that's why we are storing value_size separately in the first
   * place). */
  if(!local_value) {
    local_value = std::make_shared<string>(capacity(),'0'); 
    if(remote_value) {
      local_value->replace(0, string::npos, *remote_value);
      remote_value.reset();
    }
  }  
  if(value_size > local_value->size())
    local_value->resize(capacity());
  
  /* Copy data and remember write access. */
  local_value->replace(offset, length, buffer, length);
  updates.push_back(std::pair<off_t, size_t>(offset, length));
}

void DataBlock::truncate(off_t offset)
{
  std::lock_guard<std::mutex> lock(mutex);

  if (offset < 0) throw std::invalid_argument("negative offset");
  if (offset > cluster->limits().max_value_size)
    throw std::invalid_argument("attempting to truncate past cluster limits");

  value_size = (size_t) offset;
  updates.push_back(std::pair<off_t, size_t>(offset, 0));
}

void DataBlock::flush()
{
  std::lock_guard<std::mutex> lock(mutex);
    
  KineticStatus status(StatusCode::CLIENT_INTERNAL_ERROR, "invalid");
  do{
    if(status.statusCode() == StatusCode::REMOTE_VERSION_MISMATCH)
      getRemoteValue();

    if(local_value){
      if(value_size != local_value->size())
        local_value->resize(value_size);
      status = cluster->put(key, version, local_value, false, version);
    }
    else{
      if(!remote_value)
        remote_value = std::make_shared<const string>();       
      status = cluster->put(key, version, remote_value, false, version);
    }    
  } while (status.statusCode() == StatusCode::REMOTE_VERSION_MISMATCH); 

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

  return value_size;
}
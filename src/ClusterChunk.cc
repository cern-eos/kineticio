#include "ClusterChunk.hh"
#include "ClusterInterface.hh"
#include "LoggingException.hh"
#include <algorithm>
#include <errno.h>

using std::unique_ptr;
using std::shared_ptr;
using std::make_shared;
using std::string;
using std::chrono::system_clock;
using kinetic::KineticStatus;
using kinetic::StatusCode;
using namespace kio;

const std::chrono::milliseconds ClusterChunk::expiration_time(1000);


ClusterChunk::ClusterChunk(std::shared_ptr<ClusterInterface> c,
    const std::shared_ptr<const std::string> k, bool skip_initial_get) :
        cluster(c), key(k), version(), value(make_shared<string>()),
        timestamp(), updates()
{
  if(!cluster) throw std::invalid_argument("no cluster supplied");
  if(skip_initial_get == false)
    getRemoteValue();
}

ClusterChunk::~ClusterChunk()
{
  // take the mutex in order to prevent object deconsturction while flush 
  // operation is executed by non-owning thread.
  std::lock_guard<std::mutex> lock(mutex);
}

bool ClusterChunk::validateVersion()
{
  /* See if check is unnecessary based on expiration. */
  if(std::chrono::duration_cast<std::chrono::milliseconds>(
          system_clock::now() - timestamp)
          < expiration_time)
    return true;

  /* Check remote version & compare it to in-memory version. */
  shared_ptr<const string> remote_version;
  shared_ptr<const string> remote_value;
  KineticStatus status = cluster->get(key, true, remote_version, remote_value);

   /*If no version is set, the entry has never been flushed. In this case,
     not finding an entry with that key in the cluster is expected. */
  if( (!version && status.statusCode() == StatusCode::REMOTE_NOT_FOUND) ||
      (status.ok() && remote_version && version && *version == *remote_version)
    ){
      /* In memory version equals remote version. Remember the time. */
      timestamp = system_clock::now();
      return true;
  }
  return false;
}

void ClusterChunk::getRemoteValue()
{
  std::shared_ptr<const string> remote_value;
  auto status = cluster->get(key, false, version, remote_value);

  if(!status.ok() && status.statusCode() != StatusCode::REMOTE_NOT_FOUND)
    throw LoggingException(EIO,__FUNCTION__,__FILE__,__LINE__,
            "Attempting to read key '"+ *key+"' from cluster returned error "
            "message '"+status.message()+"'");

  /* We read in the current value from the drive. Remember the time. */
  timestamp = system_clock::now();

  /* If remote is not available, keep the current value. */
  if(status.statusCode() == StatusCode::REMOTE_NOT_FOUND)
    return;

  /* Merge all updates done on the local data copy (data) into the freshly
     read-in data copy. */ 
  auto merged_value = make_shared<string>(*remote_value);
  merged_value->resize(std::max(remote_value->size(), value->size()));

  for (auto iter = updates.begin(); iter != updates.end(); ++iter){
    auto update = *iter;
    if(update.second)
      merged_value->replace(update.first, update.second, *value, update.first, update.second);
    else
      merged_value->resize(update.first);
  }
  value = merged_value;
}

void ClusterChunk::read(char* const buffer, off_t offset, size_t length)
{
  std::lock_guard<std::mutex> lock(mutex);

  if(buffer==NULL) throw std::invalid_argument("null buffer supplied");
  if(offset<0) throw std::invalid_argument("negative offset");
  if(offset+length > cluster->limits().max_value_size)
    throw std::invalid_argument("attempting to read past cluster limits");

  /*Ensure data is not too stale to read.*/
  if(!validateVersion())
    getRemoteValue();
  
  /* return 0s if client reads non-existing data (e.g. file with holes) */
  if(offset+length > value->size())
    memset(buffer,0,length);

  if(value->size()>(size_t)offset){
    size_t copy_length = std::min(length, (size_t)(value->size()-offset));
    value->copy(buffer, copy_length, offset);
  }
}

void ClusterChunk::write(const char* const buffer, off_t offset, size_t length)
{
  std::lock_guard<std::mutex> lock(mutex);

  if(buffer==NULL) throw std::invalid_argument("null buffer supplied");
  if(offset<0) throw std::invalid_argument("negative offset");
  if(offset+length > cluster->limits().max_value_size)
    throw std::invalid_argument("attempting to write past cluster limits");

  /* Set new entry size. */
  value->resize(std::max((size_t) offset + length, value->size()));

  /* Copy data and remember write access. */
  value->replace(offset, length, buffer, length);
  updates.push_back(std::pair<off_t, size_t>(offset, length));
}

void ClusterChunk::truncate(off_t offset)
{
  std::lock_guard<std::mutex> lock(mutex);

  if(offset<0) throw std::invalid_argument("negative offset");
  if(offset > cluster->limits().max_value_size)
    throw std::invalid_argument("attempting to truncate past cluster limits");

  value->resize(offset);
  updates.push_back(std::pair<off_t, size_t>(offset, 0));
}

void ClusterChunk::flush()
{
  std::lock_guard<std::mutex> lock(mutex);

  auto status = cluster->put(key,version,value,false,version);
  while(status.statusCode() == StatusCode::REMOTE_VERSION_MISMATCH){
    getRemoteValue();
    status = cluster->put(key,version,value,false,version);
  }

  if (!status.ok())
    throw LoggingException(EIO,__FUNCTION__,__FILE__,__LINE__,
        "Attempting to write key '"+ *key+"' to the cluster returned error "
        "message '"+status.message()+"'");

  /* Success... we can forget about in-memory changes and set timestamp
     to current time. */
  updates.clear();
  timestamp = system_clock::now();
}

bool ClusterChunk::dirty() const
{
  std::lock_guard<std::mutex> lock(mutex);
  if(!version)
    return true;
  return !updates.empty();
}

int ClusterChunk::size()
{
  std::lock_guard<std::mutex> lock(mutex);

  /* Ensure size is not too stale. */
  if(!validateVersion())
    getRemoteValue();

  return value->size();
}
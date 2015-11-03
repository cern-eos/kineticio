//------------------------------------------------------------------------------
//! @file DataBlock.hh
//! @author Paul Hermann Lensing
//! @brief High(er) level API for Cluster keys.
//------------------------------------------------------------------------------
#ifndef KINETICIO_DATABLOCK_HH
#define KINETICIO_DATABLOCK_HH

/*----------------------------------------------------------------------------*/
#include <memory>
#include <chrono>
#include <string>
#include <mutex>
#include <list>
#include "ClusterInterface.hh"
/*----------------------------------------------------------------------------*/

namespace kio {

//------------------------------------------------------------------------------
//! High(er) level API for cluster keys. Handles incremental updates and
//! resolves concurrency on block-basis. For multi-block atomic writes the
//! caller will have to do appropriate locking himself. Block size depends on
//! cluster configuration. Is threadsafe to enable background flushing.
//------------------------------------------------------------------------------
class DataBlock
{
  friend class DataCache;
public:
  //! Initialized to 1 second staleness
  static const std::chrono::milliseconds expiration_time;

  //! Enum for different initialization modes
  enum class Mode { STANDARD, CREATE };

public:
  //--------------------------------------------------------------------------
  //! Reading is guaranteed up-to-date within expiration_time limits. Note that
  //! any read up to the value size limit of the assigned cluster is legal. If
  //! nothing has been written to the requested memory region, 0s will be
  //! returned.
  //!
  //! @param buffer output buffer
  //! @param offset offset in the block to start reading
  //! @param length number of bytes to read
  //--------------------------------------------------------------------------
  void read(char* const buffer, off_t offset, size_t length);

  //--------------------------------------------------------------------------
  //! Writing in-memory only, never flushes to the backend. Any write up to the
  //! value size limit of the assigned cluster is legal. Writes do not have to
  //! be consecutive.
  //!
  //! @param buffer input buffer
  //! @param offset offset in the block to start writing
  //! @param length number of bytes to write
  //--------------------------------------------------------------------------
  void write(const char* const buffer, off_t offset, size_t length);

  //--------------------------------------------------------------------------
  //! Truncate in-memory only, never flushes to the backend storage.
  //!
  //! @param offset the new size
  //--------------------------------------------------------------------------
  void truncate(off_t offset);

  //--------------------------------------------------------------------------
  //! Flush flushes all changes to the backend.
  //--------------------------------------------------------------------------
  void flush();

  //--------------------------------------------------------------------------
  //! Return the actual value size. Is up-to-date within expiration_time limits.
  //!
  //! @return size in bytes of underlying value
  //--------------------------------------------------------------------------
  std::size_t size();

  //--------------------------------------------------------------------------
  //! Return the maximum value size.
  //!
  //! @return size in bytes of underlying value
  //--------------------------------------------------------------------------
  std::size_t capacity() const;

  //--------------------------------------------------------------------------
  //! Test for your flushing needs. Block is considered dirty if it is either
  //! freshly created or it has been written to since its last flush.
  //!
  //! @return true if dirty, false otherwise.
  //--------------------------------------------------------------------------
  bool dirty() const;

  //--------------------------------------------------------------------------
  //! The identity string of a block combines the set cluster and key to form
  //! an identifier. As this identifier depends on the cluster object it will 
  //! not be consistent for multiple cluster objects of the same cluster. 
  //! 
  //! @return identity string 
  //--------------------------------------------------------------------------
  std::string getIdentity();
  
  //--------------------------------------------------------------------------
  //! Assign a new key-cluster combination to this data key, mainly to allow
  //! re-using existing objects to avoid unnecessary memory allocation. 
  //! This function is NOT threadsafe! No re-assigning of data key objects 
  //! while they are used. 
  //!
  //! @param cluster the cluster that this block is (to be) stored on
  //! @param key the name of the block
  //! @param mode if mode::create assume that the key does not yet exist
  //--------------------------------------------------------------------------
  void reassign(std::shared_ptr<ClusterInterface> cluster,
                std::shared_ptr<const std::string> key,
                Mode mode = Mode::STANDARD
  );

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param cluster the cluster that this block is (to be) stored on
  //! @param key the name of the block
  //! @param mode if mode::create assume that the key does not yet exist
  //--------------------------------------------------------------------------
  explicit DataBlock(std::shared_ptr<ClusterInterface> cluster,
                     std::shared_ptr<const std::string> key,
                     Mode mode = Mode::STANDARD
  );

  //--------------------------------------------------------------------------
  //! Destructor.
  //--------------------------------------------------------------------------
  ~DataBlock();

private:
  //--------------------------------------------------------------------------
  //! Validate the in-memory version against the version stored in the cluster
  //! assigned to this block.
  //!
  //! @return true if remote version could be verified to be equal, false
  //!         otherwise
  //--------------------------------------------------------------------------
  bool validateVersion();

  //--------------------------------------------------------------------------
  //! (Re)reads the value from the backend, merges in any existing changes made
  //! via write and / or truncate on the local copy of the value.
  //--------------------------------------------------------------------------
  void getRemoteValue();
  
private:
  //! setting the block mode can increase performance by preventing unnecessary
  //! I/O in some cases.
  Mode mode;

  //! cluster this block is (to be) stored on
  std::shared_ptr<ClusterInterface> cluster;

  //! the key of the block
  std::shared_ptr<const std::string> key;

  //! the latest known version of the key that is stored in the cluster
  std::shared_ptr<const std::string> version;
  
  //! the latest known data of the key that is stored in the cluster
  std::shared_ptr<const std::string> remote_value; 

  //! if data is written, the local_value will replace the remote value 
  std::shared_ptr<std::string> local_value;
  
  //! keeping track of the value size, since value may be pre-allocated to maximum size for efficiency
  std::size_t value_size;

  //! a list of bit-regions that have been changed since this data block has
  //! last been flushed
  std::list<std::pair<off_t, size_t> > updates;

  //! time the block was last verified to be up to date
  std::chrono::system_clock::time_point timestamp;
  
  //! thread-safety
  mutable std::mutex mutex;
};

}

#endif


//------------------------------------------------------------------------------
//! @file FileIo.hh
//! @author Paul Hermann Lensing
//! @brief Class used for doing Kinetic IO operations
//------------------------------------------------------------------------------
#ifndef __KINETICFILEIO__HH__
#define __KINETICFILEIO__HH__

/*----------------------------------------------------------------------------*/
#include "FileIoInterface.hh"
#include "ClusterInterface.hh"
#include "ClusterChunkCache.hh"
#include "ClusterChunk.hh"
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <queue>
#include <list>

namespace kio{

//------------------------------------------------------------------------------
//! Class used for doing Kinetic IO operations, mirroring FileIo interface
//! except for using exceptions instead of return codes.
//------------------------------------------------------------------------------
class FileIo : public FileIoInterface {
  friend class ClusterChunkCache;
public:
//--------------------------------------------------------------------------
  //! Open file
  //!
  //! @param path file path
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  void Open (const std::string& path, int flags, mode_t mode = 0,
    const std::string& opaque = "", uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Read from file - sync
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param timeout timeout value
  //! @return number of bytes read
  //--------------------------------------------------------------------------
  int64_t Read (long long offset, char* buffer, int length,
      uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Write to file - sync
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //! @return number of bytes written
  //--------------------------------------------------------------------------
  int64_t Write (long long offset, const char* buffer,
      int length, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  void Truncate (long long offset, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  void Remove (uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  void Sync (uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  void Close (uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  void Stat (struct stat* buf, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state
  //!
  //! @param path to statfs
  //! @param statfs return struct
  //----------------------------------------------------------------------------
  void Statfs (const char* path, struct statfs* statFs);

  //--------------------------------------------------------------------------
  //! Open a curser to traverse a storage system
  //! @param subtree where to start traversing
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------
  void* ftsOpen(std::string subtree);

  //--------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //! @param fts_handle cursor obtained by ftsOpen
  //! @return returns full path (including mountpoint) for the next path
  //!         indicated by traversal cursor, empty string if there is no next
  //--------------------------------------------------------------------------
  std::string ftsRead(void* fts_handle);

  //--------------------------------------------------------------------------
  //! Close a traversal cursor
  //! @param fts_handle cursor to close
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //--------------------------------------------------------------------------
  int ftsClose(void* fts_handle);

  //--------------------------------------------------------------------------
  //! Constructor
  //! @param cache_capacity maximum cache size
  //--------------------------------------------------------------------------
  explicit FileIo ();

  FileIo (const FileIo&) = delete;
  FileIo& operator = (const FileIo&) = delete;

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~FileIo ();

private:
  enum rw {READ, WRITE};
  int64_t ReadWrite (long long off, char* buffer, int length, rw mode, uint16_t timeout = 0);

private:
  class LastChunkNumber {

  public:
     //--------------------------------------------------------------------------
     //! Checks if the chunk number stored in last_chunk_number is still valid,
     //! if not it will query the drive to obtain the up-to-date last chunk and
     //! store it (so it can get requested with get() by the user).
     //-------------------------------------------------------------------------
     void verify();

     //-------------------------------------------------------------------------
     //! Get the chunk number of the last chunk.
     //!
     //! @return currently set last chunk number
     //-------------------------------------------------------------------------
     int get() const;

     //-------------------------------------------------------------------------
     //! Set the supplied chunk number as last chunk.
     //!
     //! @param chunk_number the chunk number to be set
     //-------------------------------------------------------------------------
     void set(int chunk_number);

     //-------------------------------------------------------------------------
     //! Constructor
     //!
     //! @param parent reference to the enclosing KineticFileIo object
     //-------------------------------------------------------------------------
     explicit LastChunkNumber(FileIo & parent);

     //-------------------------------------------------------------------------
     //! Destructor.
     //-------------------------------------------------------------------------
     ~LastChunkNumber();

  private:
      //! reference to the enclosing KineticFileIo object
      FileIo & parent;

      //! currently set last chunk number
      int last_chunk_number;

      //! time point it was verified that the last_chunk_number is correct
      //! (another client might have created a later chunk)
      std::chrono::system_clock::time_point last_chunk_number_timestamp;
  };

private:
  //! we don't want to have to look in the drive map for every access...
  std::shared_ptr<ClusterInterface> cluster;

  //! cache functionality
  ClusterChunkCache& cache;

  //! keep track of the last chunk to answer stat requests reasonably
  LastChunkNumber lastChunkNumber;

  //! the full kinetic path of the form kinetic:clusterID:name
  std::string obj_path;

  //! the base name for data chunks of this object
  std::string chunk_basename;
};

}
#endif  // __KINETICFILEIO__HH__

//------------------------------------------------------------------------------
//! @file FileIoInterface.hh
//! @author Paul Hermann Lensing
//! @brief EOS-style FileIo interface.
//------------------------------------------------------------------------------
#ifndef __KINETICIO_FILEIOINTERFACE__HH__
#define __KINETICIO_FILEIOINTERFACE__HH__

#include <string>
#include <sys/vfs.h>
#include <sys/stat.h>

#ifndef SFS_O_CREAT
#define SFS_O_CREAT 0x100
#endif 

namespace kio {

//------------------------------------------------------------------------------
//! EOS-style FileIo interface.
//------------------------------------------------------------------------------
class FileIoInterface {
public:
  //--------------------------------------------------------------------------
  //! Open file
  //!
  //! @param path file path
  //! @param flags open flags, use SFS_O_CREAT (0x100) to signify create
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  virtual void Open (const std::string& path, int flags, mode_t mode = 0,
    const std::string& opaque = "", uint16_t timeout = 0) = 0;

  //--------------------------------------------------------------------------
  //! Read from file - sync
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param timeout timeout value
  //! @return number of bytes read
  //--------------------------------------------------------------------------
  virtual int64_t Read (long long offset, char* buffer, int length,
      uint16_t timeout = 0) = 0;

  //--------------------------------------------------------------------------
  //! Write to file - sync
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //! @return number of bytes written
  //--------------------------------------------------------------------------
  virtual int64_t Write (long long offset, const char* buffer,
      int length, uint16_t timeout = 0) = 0;

  //--------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  virtual void Truncate (long long offset, uint16_t timeout = 0) = 0;

  //--------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  virtual void Remove (uint16_t timeout = 0) = 0;

  //--------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  virtual void Sync (uint16_t timeout = 0) = 0;

  //--------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  virtual void Close (uint16_t timeout = 0) = 0;

  //--------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  virtual void Stat (struct stat* buf, uint16_t timeout = 0) = 0;

  //----------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state
  //!
  //! @param path to statfs
  //! @param statfs return struct
  //----------------------------------------------------------------------------
  virtual void Statfs (const char* path, struct statfs* statFs) = 0;

  //--------------------------------------------------------------------------
  //! Open a curser to traverse a storage system
  //! @param subtree where to start traversing
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------
  virtual void* ftsOpen(std::string subtree) = 0;

  //--------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //! @param fts_handle cursor obtained by ftsOpen
  //! @return returns full path (including mountpoint) for the next path
  //!         indicated by traversal cursor, empty string if there is no next
  //--------------------------------------------------------------------------
  virtual std::string ftsRead(void* fts_handle) = 0;

  //--------------------------------------------------------------------------
  //! Close a traversal cursor
  //! @param fts_handle cursor to close
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //--------------------------------------------------------------------------
  virtual int ftsClose(void* fts_handle) = 0;

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  virtual ~FileIoInterface (){};
};

}

#endif  // __KINETICIO_FILEIOINTERFACE__HH__

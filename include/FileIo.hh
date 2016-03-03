//------------------------------------------------------------------------------
//! @file FileIo.hh
//! @author Paul Hermann Lensing
//! @brief Class used for doing Kinetic IO operations
//------------------------------------------------------------------------------

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

#ifndef KINETICIO_FILEIO_HH
#define KINETICIO_FILEIO_HH

/*----------------------------------------------------------------------------*/
#include "FileIoInterface.hh"
#include "ClusterInterface.hh"
#include "DataCache.hh"
#include "DataBlock.hh"
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <queue>
#include <list>

namespace kio {

//------------------------------------------------------------------------------
//! Class used for doing Kinetic IO operations, oriented at EOS FileIo interface
//! but using exceptions to return error codes.
//------------------------------------------------------------------------------
class FileIo : public FileIoInterface {
  friend class DataCache;

public:
  //--------------------------------------------------------------------------
  //! Open file
  //!
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  void Open(int flags, mode_t mode = 0, const std::string& opaque = "", uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Read from file - sync
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param timeout timeout value
  //! @return number of bytes read
  //--------------------------------------------------------------------------
  int64_t Read(long long offset, char* buffer, int length, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Write to file - sync
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //! @return number of bytes written
  //--------------------------------------------------------------------------
  int64_t Write(long long offset, const char* buffer, int length, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  void Truncate(long long offset, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  void Remove(uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  void Sync(uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  void Close(uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //--------------------------------------------------------------------------
  void Stat(struct stat* buf, uint16_t timeout = 0);

  //---------------------------------------------------------------------------
  //! Set an attribute
  //---------------------------------------------------------------------------
  void attrSet(std::string name, std::string value);

  //---------------------------------------------------------------------------
  //! Delete an attribute by name
  //---------------------------------------------------------------------------
  void attrDelete(std::string name);

  //---------------------------------------------------------------------------
  //! Get an attribute by name
  //---------------------------------------------------------------------------
  std::string attrGet(std::string name);

  //---------------------------------------------------------------------------
  //! List all attributes for this file
  //---------------------------------------------------------------------------
  std::vector<std::string> attrList();

  //----------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state
  //!
  //! @param path to statfs
  //! @param statfs return struct
  //----------------------------------------------------------------------------
  void Statfs(struct statfs* statFs);

  //---------------------------------------------------------------------------
  //! Get list of all files under the specified subtree.
  //!
  //! @param subtree start point in the directory tree for listing files
  //! @param max the maximum number of elements to return, a return of less
  //!   elements indicates that no more files exists under the subtree.
  //! @return list of up to max size of elements existing in the subtree
  //---------------------------------------------------------------------------
  std::vector<std::string> ListFiles(std::string subtree, size_t max);

  //--------------------------------------------------------------------------
  //! Constructor
  //! @param url the kinetic url of the form kinetic://clusterId/path
  //--------------------------------------------------------------------------
  explicit FileIo(const std::string& url);

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~FileIo();

  FileIo(const FileIo&) = delete;

  FileIo& operator=(const FileIo&) = delete;

private:
  enum rw {
      READ, WRITE
  };

  int64_t ReadWrite(long long off, char* buffer, int length, rw mode, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Attempt to prefetch blocks based on the provided block number. If no
  //! access pattern can be detected, no io-threads are available or the cache
  //! utilization is very high, read-ahead will be skipped.
  //!
  //! @param blocknumber the data block currently being read / written
  //--------------------------------------------------------------------------
  void scheduleReadahead(int blocknumber);

  //--------------------------------------------------------------------------
  //! Schedule a background flush for the supplied data block.
  //!
  //! @param data the data to flush
  //--------------------------------------------------------------------------
  void scheduleFlush(std::shared_ptr<kio::DataBlock> data);

  //--------------------------------------------------------------------------
  //! Execute a flush operation. As this function is intended to be run
  //! by one of the background io threads, a possibly thrown exception will
  //! be stored in this FileIo's exception queue.
  //!
  //! @param data the data to flush to the backend
  //--------------------------------------------------------------------------
  void doFlush(std::shared_ptr<kio::DataBlock> data);

  //--------------------------------------------------------------------------
  //! Verify the eof_blocknumber attribute.
  //--------------------------------------------------------------------------
  void verify_eof();

  //--------------------------------------------------------------------------
  //! Check for the last block on the backend cluster.
  //! @return the last block number
  //--------------------------------------------------------------------------
  int get_eof_backend();

  /* protected instead of private to allow mocking in cache performance testing */
protected:
  //! we don't want to have to look in the drive map for every access...
  std::shared_ptr<ClusterInterface> cluster;

  //! read-ahead
  PrefetchOracle prefetchOracle;

  //! the size_hint attribute that was read in during open
  int size_hint;

  //! the currently last block number
  int eof_blocknumber;

  //! time point it was verified that eof_blocknumber is in sync with the backend (multi-clients)
  std::chrono::system_clock::time_point eof_verification_time;

  //! Exceptions occurring during background execution are stored and thrown at the next request.
  std::queue<std::system_error> exceptions;

  //! Thread safety when accessing exceptions
  std::mutex exception_mutex;

  //! true if file has been opened successfully
  bool opened;

  //! the extracted path from the full path 'kinetic:clusterId:path'
  std::string path;
};

}
#endif  // __KINETICFILEIO__HH__

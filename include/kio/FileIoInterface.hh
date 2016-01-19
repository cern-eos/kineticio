//------------------------------------------------------------------------------
//! @file FileIoInterface.hh
//! @author Paul Hermann Lensing
//! @brief EOS-style FileIo interface.
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

#ifndef __KINETICIO_FILEIOINTERFACE__HH__
#define __KINETICIO_FILEIOINTERFACE__HH__

#include <string>
#include <vector>
#include <sys/vfs.h>
#include <sys/stat.h>

#ifndef SFS_O_CREAT
#define SFS_O_CREAT 0x100
#endif

namespace kio {


class FileIoInterface {
public:
  //---------------------------------------------------------------------------
  //! Get list of all files under the specified subtree.
  //!
  //! @param subtree start point in the directory tree for listing files. Must
  //!   contain the path associated with this io object
  //! @param max the maximum number of elements to return, a return of less
  //!   elements indicates that no more files exists under the subtree.
  //! @return list of up to max elements existing in the subtree
  //---------------------------------------------------------------------------
  virtual std::vector<std::string> ListFiles(std::string subtree, size_t max) = 0;

  //----------------------------------------------------------------------------
  //! Open file
  //!
  //! @param flags open flags, use SFS_O_CREAT (0x100) to signify create
  //! @param mode open mode (ignored)
  //! @param opaque opaque information (ignored)
  //! @param timeout timeout value
  //---------------------------------------------------------------------------
  virtual void Open(int flags, mode_t mode = 0, const std::string& opaque = "", uint16_t timeout = 0) = 0;

  //---------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //---------------------------------------------------------------------------
  virtual void Close(uint16_t timeout = 0) = 0;

  //---------------------------------------------------------------------------
  //! Read from file
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param timeout timeout value
  //! @return number of bytes read
  //---------------------------------------------------------------------------
  virtual int64_t Read(long long offset, char* buffer, int length, uint16_t timeout = 0) = 0;

  //---------------------------------------------------------------------------
  //! Write to file
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //! @return number of bytes written
  //---------------------------------------------------------------------------
  virtual int64_t Write(long long offset, const char* buffer, int length, uint16_t timeout = 0) = 0;

  //---------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //---------------------------------------------------------------------------
  virtual void Truncate(long long offset, uint16_t timeout = 0) = 0;

  //---------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //---------------------------------------------------------------------------
  virtual void Sync(uint16_t timeout = 0) = 0;

  //---------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //---------------------------------------------------------------------------
  virtual void Stat(struct stat* buf, uint16_t timeout = 0) = 0;

  //---------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //---------------------------------------------------------------------------
  virtual void Remove(uint16_t timeout = 0) = 0;

  //---------------------------------------------------------------------------
  //! Set an attribute
  //---------------------------------------------------------------------------
  virtual void attrSet(std::string name, std::string value) = 0;

  //---------------------------------------------------------------------------
  //! Delete an attribute by name
  //---------------------------------------------------------------------------
  virtual void attrDelete(std::string name) = 0;

  //---------------------------------------------------------------------------
  //! Get an attribute by name
  //---------------------------------------------------------------------------
  virtual std::string attrGet(std::string name) = 0;

  //---------------------------------------------------------------------------
  //! List all attributes for this file
  //---------------------------------------------------------------------------
  virtual std::vector<std::string> attrList() = 0;

  //---------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state
  //!
  //! @param statfs return struct
  //---------------------------------------------------------------------------
  virtual void Statfs(struct statfs* statFs) = 0;

  //---------------------------------------------------------------------------
  //! Destructor
  //---------------------------------------------------------------------------
  virtual ~FileIoInterface()
  { };
};

}

#endif  // __KINETICIO_FILEIOINTERFACE__HH__

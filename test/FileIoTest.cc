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

#include "KineticIoFactory.hh"
#include "SimulatorController.h"
#include <unistd.h>
#include <fcntl.h>
#include <FileIo.hh>
#include <Logging.hh>
#include "catch.hpp"

using namespace kio;

SCENARIO("KineticIo Integration Test", "[Io]")
{

  auto& c = SimulatorController::getInstance();
  REQUIRE(c.reset());
  kio::KineticIoFactory::reloadConfiguration();

  int buf_size = 64;
  char write_buf[] = "rcPOa12L3nhN5Cgvsa6Jlr3gn58VhazjA6oSpKacLFYqZBEu0khRwbWtEjge3BUA";
  char read_buf[buf_size];
  char null_buf[buf_size];
  memset(read_buf, 0, buf_size);
  memset(null_buf, 0, buf_size);

  GIVEN("Wrong urls") {
    THEN("fileio object creation throws with EINVAL on illegally constructed urls") {
      std::string path("path");
      REQUIRE_THROWS_AS(kio::KineticIoFactory::makeFileIo(path), std::system_error);
      try {
        kio::KineticIoFactory::makeFileIo(path);
      } catch (const std::system_error& e) {
        REQUIRE((e.code().value() == EINVAL));
      }
    }
    THEN("fileio object creation throws with ENODEV on nonexisting clusters") {
      std::string path("kinetic://thisdoesntexist/file");
      REQUIRE_THROWS_AS(kio::KineticIoFactory::makeFileIo(path), std::system_error);
      try {
        kio::KineticIoFactory::makeFileIo(path);
      } catch (const std::system_error& e) {
        REQUIRE((e.code().value() == ENODEV));
      }
    }
  }

  GIVEN ("A valid path, but no existing file.") {
    std::string base_url("kinetic://Cluster1/");
    std::string full_url(base_url + "filename");
    auto fileio = kio::KineticIoFactory::makeFileIo(full_url);
    REQUIRE(fileio);

    THEN("All file IO operations throw when attempted on unopened file") {
      REQUIRE_THROWS_AS(fileio->Read(0, read_buf, buf_size), std::system_error);
      REQUIRE_THROWS_AS(fileio->Write(0, write_buf, buf_size), std::system_error);
      REQUIRE_THROWS_AS(fileio->Truncate(0), std::system_error);
      REQUIRE_THROWS_AS(fileio->Remove(), std::system_error);
    }

    THEN("Attempting to open without create flag fails with ENOENT.") {
      try {
        fileio->Open(0);
      } catch (const std::system_error& e) {
        REQUIRE((e.code().value() == ENOENT));
      }
    }

    THEN("Statfs succeeds") {
      /* wait for io stats to update in freshly generated cluster */
      usleep(100 * 1000);
      struct statfs sfs;
      REQUIRE_NOTHROW(fileio->Statfs(&sfs));
      REQUIRE((sfs.f_bavail > 0));
    }

    THEN("ListFiles returns an empty vector.") {
      auto list = fileio->ListFiles(full_url, 100);
      REQUIRE(list.empty());
    }
  }

  GIVEN("A file is created.") {
    std::string base_url("kinetic://Cluster2/");
    std::string full_url(base_url + "filename");
    auto fileio = kio::KineticIoFactory::makeFileIo(full_url);

    REQUIRE_NOTHROW(fileio->Open(SFS_O_CREAT));

    THEN("Trying to create the same file again fails with EEXIST") {
      try {
        KineticIoFactory::makeFileIo(full_url)->Open(SFS_O_CREAT);
      } catch (const std::system_error& e) {
        REQUIRE((e.code().value() == EEXIST));
      }
    }

    WHEN("A buffer is read into memory") {
      int size = 20;
      char abuf[size];
      char bbuf[size];

      int fd = open("/dev/random", O_RDONLY);
      read(fd, abuf, size);
      close(fd);
      abuf[10] = 0;

      THEN("We can write it to the filio object & read it straight away.") {
        REQUIRE((fileio->Write(0, abuf, size) == size));
        REQUIRE((fileio->Read(0, bbuf, size) == size));
        REQUIRE((memcmp(abuf, bbuf, size) == 0));
        fileio->Close();

        AND_THEN("We can read it in again after reopening the object.") {
          REQUIRE_NOTHROW(fileio->Open(0));
          REQUIRE((fileio->Read(0, bbuf, size) == size));
          REQUIRE((memcmp(abuf, bbuf, size) == 0));
        }
      }
    }

    THEN("ListFiles returns the file name") {
      auto baseio = kio::KineticIoFactory::makeFileIo(base_url);
      WHEN("Using a baseio object with the full url.") {
        auto list = baseio->ListFiles(full_url, 100);
        REQUIRE((list.size() == 1));
        REQUIRE((list.front() == full_url));
      }
      WHEN("Using a baseio object with the base url.") {
        auto list = baseio->ListFiles(base_url, 100);
        REQUIRE((list.size() == 1));
        REQUIRE((list.front() == full_url));
      }
      WHEN("Using the same object wiht the full url") {
        auto list = fileio->ListFiles(full_url, 100);
        REQUIRE((list.size() == 1));
        REQUIRE((list.front() == full_url));
      }
    }

    THEN("ListFiles throws when using a non-child url for the io object"){
      REQUIRE_THROWS(fileio->ListFiles(base_url,100));
    }

    THEN("Attempting to read an empty file reads 0 bytes.") {
      REQUIRE((fileio->Read(0, read_buf, buf_size) == 0));
    }

    THEN("Attempting to read an empty file with and offset reads 0 bytes.") {
      REQUIRE((fileio->Read(199, read_buf, buf_size) == 0));
    }

    THEN("Writing is possible from object start.") {
      REQUIRE((fileio->Write(0, write_buf, buf_size) == buf_size));

      AND_THEN("Written data can be read in again.") {
        REQUIRE((fileio->Read(0, read_buf, buf_size) == buf_size));
        REQUIRE((memcmp(write_buf, read_buf, buf_size) == 0));
      }
    }

    THEN("Writing is possible from an offset.") {
      REQUIRE((fileio->Write(77777777, write_buf, buf_size) == buf_size));

      AND_THEN("Written data can be read in again.") {
        REQUIRE((fileio->Read(77777777, read_buf, buf_size) == buf_size));
        REQUIRE((memcmp(write_buf, read_buf, buf_size) == 0));
      }

      AND_THEN("Reading with offset < filesize but offset+length > filesize only reads to filesize limits") {
        REQUIRE((fileio->Read(77777777 + buf_size / 2, read_buf, buf_size) == buf_size / 2));
      }

      AND_THEN("Reading data before the offset is possible and returns 0s (file with holes)") {
        REQUIRE((fileio->Read(66666666, read_buf, buf_size) == buf_size));
        REQUIRE((memcmp(null_buf, read_buf, buf_size) == 0));
      }
    }

    THEN("Stat should succeed and report a file size of 0") {
      struct stat stbuf;
      REQUIRE_NOTHROW(fileio->Stat(&stbuf));
      REQUIRE((stbuf.st_blocks == 1));
      REQUIRE((stbuf.st_blksize == 2 * 1024 * 1024));
      REQUIRE((stbuf.st_size == 0));
    }

    WHEN("Truncate is called to change the file size.") {
      for (int block = 3; block >= 0; block--) {
        for (int odd = 0; odd <= 1; odd++) {
          size_t size = 2 * 1024 * 1024 * block + odd;
          REQUIRE_NOTHROW(fileio->Truncate(size));
          THEN("stat succeeds and returns the truncated size") {
            struct stat stbuf;
            REQUIRE_NOTHROW(fileio->Stat(&stbuf));
            REQUIRE(((size_t) stbuf.st_size == size));
          }
        }
      }
    }

    WHEN("The Io object grows over 100 blocks and is closed") {
      size_t size = 2*1024*1024*512;
      REQUIRE_NOTHROW(fileio->Truncate(size));
      fileio->Close();
      THEN("stat succeeds and returns the correct size") {
           struct stat stbuf;
           REQUIRE_NOTHROW(fileio->Stat(&stbuf));
           REQUIRE(((size_t) stbuf.st_size == size));
      }
    }

    AND_WHEN("The file is removed via a second io object.") {
      auto fileio_2nd = KineticIoFactory::makeFileIo(full_url);
      REQUIRE_NOTHROW(fileio_2nd->Open(0));
      REQUIRE_NOTHROW(fileio_2nd->Remove());

      THEN("The the change will not immediately be visible to the first io object") {
        struct stat stbuf;
        REQUIRE_NOTHROW(fileio->Stat(&stbuf));
      }

      THEN("Calling stat will fail with ENOENT after expiration time has run out.") {
        usleep(1000 * 1000);
        struct stat stbuf;
        REQUIRE_THROWS(fileio->Stat(&stbuf));
      }
    }

    AND_WHEN("Writing data across multiple blocks.") {
      const size_t capacity = 2 * 1024 * 1024;
      REQUIRE((fileio->Write(capacity - 32, write_buf, buf_size) == buf_size));

      THEN("IO object can be synced.") {
        REQUIRE_NOTHROW(fileio->Sync());
      }

      THEN("Stat will return the number of blocks and the filesize.") {
        struct stat stbuf;
        REQUIRE_NOTHROW(fileio->Stat(&stbuf));
        REQUIRE((stbuf.st_blocks == 2));
        REQUIRE(((size_t) stbuf.st_blksize == capacity));
        REQUIRE((stbuf.st_size == stbuf.st_blksize - 32 + buf_size));

        AND_THEN("Writing again earlier in the file doesn't change the stat size.") {
          REQUIRE((fileio->Write(10, write_buf, buf_size) == buf_size));
          REQUIRE_NOTHROW(fileio->Stat(&stbuf));
          REQUIRE((stbuf.st_blocks == 2));
          REQUIRE(((size_t) stbuf.st_blksize == capacity));
          REQUIRE((stbuf.st_size == stbuf.st_blksize - 32 + buf_size));
        }

        AND_THEN("Reopening the object, using it to write earlier, correct stat size is still reported.") {
          REQUIRE_NOTHROW(fileio->Close());
          REQUIRE_NOTHROW(fileio->Open(0));
          REQUIRE((fileio->Write(10, write_buf, buf_size) == buf_size));
          REQUIRE_NOTHROW(fileio->Stat(&stbuf));
          REQUIRE((stbuf.st_blocks == 2));
          REQUIRE(((size_t) stbuf.st_blksize == capacity));
          REQUIRE((stbuf.st_size == stbuf.st_blksize - 32 + buf_size));
        }
      }

      THEN("The file can can be removed again.") {
        REQUIRE_NOTHROW(fileio->Remove());
        REQUIRE_NOTHROW(fileio->Close());

        AND_THEN("ListFiles returns an empty list.") {
          auto list = fileio->ListFiles(full_url, 10000);
          REQUIRE(list.empty());
        }
      }
    }
  }
}

SCENARIO("FileIo Attribute Integration Test", "[Attr]")
{
  auto& c = SimulatorController::getInstance();
  REQUIRE(c.reset());
  kio::KineticIoFactory::reloadConfiguration();

  GIVEN("A file is created.") {
    std::string base_url("kinetic://Cluster2/");
    std::string full_url(base_url + "filename");
    auto fileio = KineticIoFactory::makeFileIo(full_url);

    REQUIRE_NOTHROW(fileio->Open(SFS_O_CREAT));

    THEN("attributes can be set and read-in again.") {
      std::string write_value = "rcPOa12L3nhN5Cgvsa6Jlr3gn58VhazjA6oSpKacLFYqZBEu0khRwbWtEjge3BUA";
      REQUIRE_NOTHROW(fileio->attrSet("name", write_value));
      REQUIRE((write_value == fileio->attrGet("name")));

      AND_THEN("We can overwrite the existing attribute") {
        std::string owrite_value = "12345";
        REQUIRE_NOTHROW(fileio->attrSet("name", owrite_value));
        REQUIRE((owrite_value == fileio->attrGet("name")));
      }

      AND_THEN("Attribute functionality remains active if the file is closed.") {
        fileio->Close();
        REQUIRE((write_value == fileio->attrGet("name")));
      }

      AND_THEN("attribute can be deleted by name.") {
        REQUIRE_NOTHROW(fileio->attrDelete("name"));
        AND_THEN("We can't get it anymore.") {
          REQUIRE_THROWS(fileio->attrGet("name"));
        }
      }

      AND_THEN("attributes are not returned by ListFiles") {
        auto list = fileio->ListFiles(full_url, 7);
        REQUIRE((list.size() == 1));
        REQUIRE((list.front() == full_url));
      }

      AND_THEN("attributes can be listed.") {
        auto list = fileio->attrList();
        REQUIRE((list.size() == 1));
        REQUIRE((list.front() == "name"));

        AND_WHEN("A file is removed") {
          REQUIRE_NOTHROW(fileio->Remove());
          THEN("Attributes are removed along with it...") {
            auto list = fileio->attrList();
            REQUIRE((list.size() == 0));
          }
        }
      }
    }

    THEN("Attempting to read in a non-existing attribute throws.") {
      REQUIRE_THROWS(fileio->attrGet("nope"));
    }

    THEN("Attempting to remove a non-existing attribute name is ok.") {
      REQUIRE_NOTHROW(fileio->attrDelete("nope"));
    }

    THEN("We can use the attr interface to request io stats") {
      auto stats = fileio->attrGet("sys.iostats");
      REQUIRE(stats.size());
    }

    THEN("We can use the attr interface to request health stats") {
      auto health = fileio->attrGet("sys.health");
      REQUIRE((health.find("redundancy_factor=1") != std::string::npos));
    }
  }
}


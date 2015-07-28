#include "catch.hpp"
#include "KineticIoFactory.hh"
#include "SimulatorController.h"
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <memory>
#include <random>
#include <kinetic/kinetic.h>

using namespace kio;

SCENARIO("KineticIo Integration Test", "[Io]"){

  auto& c = SimulatorController::getInstance();
  c.start(0);
  c.start(1);
  c.start(2);
  REQUIRE( c.reset(0) );
  REQUIRE( c.reset(1) );
  REQUIRE( c.reset(2) );

  auto fileio = Factory::uniqueFileIo();

  int  buf_size = 64;
  char write_buf[] = "rcPOa12L3nhN5Cgvsa6Jlr3gn58VhazjA6oSpKacLFYqZBEu0khRwbWtEjge3BUA";
  char read_buf[buf_size];
  char null_buf[buf_size];
  memset (read_buf, 0, buf_size);
  memset (null_buf, 0, buf_size);

  GIVEN("An illegally constructed path"){
    std::string path("path");

    THEN("Calling Open returns ENODEV"){
      REQUIRE_THROWS(fileio->Open(path.c_str(), 0));
    }

    THEN("Calling Statfs returns ENODEV"){
      struct statfs sfs;
      REQUIRE_THROWS(fileio->Statfs(path.c_str(), &sfs));
    }

    THEN("Factory function for Attribute class throws"){
      REQUIRE_THROWS( Factory::uniqueFileAttr(path.c_str()) );
    }
  }

  GIVEN ("A kio object that has not been openend."){
    std::string base_path("kinetic:Cluster1:");
    std::string path(base_path+"filename");

    THEN("All public operations (except Statfs) fail with ENXIO error code"){
      REQUIRE_THROWS(fileio->Read(0,read_buf,buf_size));
      REQUIRE_THROWS(fileio->Write(0,write_buf,buf_size));
      REQUIRE_THROWS(fileio->Truncate(0));
      REQUIRE_THROWS(fileio->Remove());
      REQUIRE_THROWS(fileio->Sync());
      REQUIRE_THROWS(fileio->Close());
    }

    THEN("Statfs succeeds"){
      struct statfs sfs;
      REQUIRE_NOTHROW(fileio->Statfs(path.c_str(), &sfs));
      // sleep 100 ms to let cluster size update after creation
      usleep(1000 * 100);
      REQUIRE_NOTHROW(fileio->Statfs(path.c_str(), &sfs));
      REQUIRE(sfs.f_bavail > 0);
    }

    THEN("Factory function for Attribute class returns 0"){
      auto a = Factory::uniqueFileAttr(path.c_str());
      REQUIRE(!a);
     }

    THEN("ftsRead returns \"\"."){
      void * handle = fileio->ftsOpen(base_path);
      REQUIRE(handle != NULL);
      REQUIRE(fileio->ftsRead(handle) == "");
      REQUIRE(fileio->ftsClose(handle) == 0);
    }
  }

  GIVEN("Open succeeds on healthy cluster"){
    std::string base_path("kinetic:Cluster2:");
    std::string path(base_path+"filename");
    REQUIRE_NOTHROW(fileio->Open(path.c_str(), 0));

    WHEN("A buffer is read into memory"){
      int size = 20;
      char abuf[size];
      char bbuf[size];

      int fd = open("/dev/random", O_RDONLY);
      read(fd, abuf, size);
      close(fd);
      abuf[10]=0;


      THEN("We can write it to the filio object."){
          REQUIRE(fileio->Write(0,abuf,size) == size);
          REQUIRE(fileio->Read(0,bbuf,size) == size);
          REQUIRE(memcmp(abuf,bbuf,size) == 0);
          fileio->Close();

          AND_THEN("We can read it in again."){
            REQUIRE_NOTHROW(fileio->Open(path.c_str(), 0));
            REQUIRE(fileio->Read(0,bbuf,size) == size);
            REQUIRE(memcmp(abuf,bbuf,size) == 0);
            fileio->Close();
          }
      }
    }

    THEN("The first ftsRead returns the full path, the second \"\"."){
      void * handle = fileio->ftsOpen(base_path);
      REQUIRE(handle != NULL);
      REQUIRE(fileio->ftsRead(handle) == path);
      REQUIRE(fileio->ftsRead(handle) == "");
      REQUIRE(fileio->ftsClose(handle) == 0);
    }

    THEN("Factory function for Attribute class succeeds"){
      auto a = Factory::uniqueFileAttr(path.c_str());
      REQUIRE(a);

      AND_THEN("attributes can be set and read-in again."){
        REQUIRE(a->Set("name", write_buf, buf_size) == true);
        size_t size = buf_size;
        REQUIRE(a->Get("name",read_buf,size) == true);
        REQUIRE(size == buf_size);
        REQUIRE(memcmp(write_buf,read_buf,buf_size) == 0);
      }
    }

    THEN("Attempting to read an empty file reads 0 bytes."){
      REQUIRE(fileio->Read(0,read_buf,buf_size) == 0);
    }

    THEN("Attempting to read an empty file with and offset reads 0 bytes."){
      REQUIRE(fileio->Read(199,read_buf,buf_size) == 0);
    }

    THEN("Writing is possible from object start."){
      REQUIRE(fileio->Write(0, write_buf, buf_size) == buf_size);

      AND_THEN("Written data can be read in again."){
        REQUIRE(fileio->Read(0, read_buf, buf_size) == buf_size);
        REQUIRE(memcmp(write_buf,read_buf,buf_size) == 0);
      }
    }

    THEN("Writing is possible from an offset."){
      REQUIRE(fileio->Write(1000000,write_buf,buf_size) == buf_size);

      AND_THEN("Written data can be read in again."){
        REQUIRE(fileio->Read(1000000, read_buf, buf_size) == buf_size);
        REQUIRE(memcmp(write_buf,read_buf,buf_size) == 0);
      }

      AND_THEN("Reading with offset < filesize but offset+length > filesize only reads to filesize limits"){
        REQUIRE(fileio->Read(1000000+buf_size/2, read_buf, buf_size) == buf_size/2);
      }

      AND_THEN("Reading data before the offset is possible and returns 0s (file with holes)"){
        REQUIRE(fileio->Read(1000000/3, read_buf, buf_size) == buf_size);
        REQUIRE(memcmp(null_buf,read_buf,buf_size) == 0);
      }
    }

    THEN("Stat should succeed and report a file size of 0"){
      struct stat stbuf;
      REQUIRE_NOTHROW(fileio->Stat(&stbuf));
      REQUIRE(stbuf.st_blocks == 1);
      REQUIRE(stbuf.st_blksize == 1024*1024);
      REQUIRE(stbuf.st_size == 0);
    }

    WHEN("Truncate is called to change the file size."){
      for(int chunk=3; chunk>=0; chunk--){
        for(int odd=0; odd<=1; odd++){
          size_t size = 1024*1024*chunk+odd;
          REQUIRE_NOTHROW(fileio->Truncate(size));
          THEN("statfs succeeds and returns the truncated size"){
              struct stat stbuf;
              REQUIRE_NOTHROW(fileio->Stat(&stbuf));
              REQUIRE(stbuf.st_size == size);
          }
        }
      }
    }

    THEN("Calling statfs on the same object is illegal,"){
      struct statfs sfs;
      REQUIRE_THROWS(fileio->Statfs(base_path.c_str(), &sfs));
    }

    AND_WHEN("The file is removed via a second io object."){
      auto fileio_2nd = Factory::uniqueFileIo();
      REQUIRE_NOTHROW(fileio_2nd->Open(path.c_str(), 0));
      REQUIRE_NOTHROW(fileio_2nd->Remove());

      THEN("The the change will not immediately be visible to the first io object"){
        struct stat stbuf;
        REQUIRE_NOTHROW(fileio->Stat(&stbuf));
      }

      THEN("Calling stat will fail with ENOENT after expiration time has run out."){
        usleep(1000*1000);
        struct stat stbuf;
        REQUIRE_THROWS(fileio->Stat(&stbuf));
      }
    }

    AND_WHEN("Writing data across multiple chunks."){
      const size_t capacity = 1024*1024;
      REQUIRE(fileio->Write(capacity-32, write_buf, buf_size) == buf_size);

      THEN("IO object can be synced."){
        REQUIRE_NOTHROW(fileio->Sync());
      }

      THEN("Stat will return the number of blocks and the filesize."){
        struct stat stbuf;
        REQUIRE_NOTHROW(fileio->Stat(&stbuf));
        REQUIRE(stbuf.st_blocks == 2);
        REQUIRE(stbuf.st_blksize == capacity);
        REQUIRE(stbuf.st_size == stbuf.st_blksize-32+buf_size);
      }

      THEN("The file can can be removed again."){
        REQUIRE_NOTHROW(fileio->Remove());
        REQUIRE_NOTHROW(fileio->Close());

        AND_THEN("ftsRead returns \"\"."){
          void * handle = fileio->ftsOpen(base_path);
          REQUIRE(handle != NULL);
          REQUIRE(fileio->ftsRead(handle) == "");
          REQUIRE(fileio->ftsClose(handle) == 0);
        }
      }
    }
  }
}

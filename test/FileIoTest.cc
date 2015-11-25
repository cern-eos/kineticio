#include "catch.hpp"
#include "KineticIoFactory.hh"
#include "SimulatorController.h"
#include <unistd.h>
#include <fcntl.h>
#include <LoggingException.hh>
#include <FileIo.hh>

using namespace kio;

SCENARIO("KineticIo Integration Test", "[Io]"){

  auto& c = SimulatorController::getInstance();
  c.start(0);
  c.start(1);
  c.start(2);
  REQUIRE( c.reset(0) );
  REQUIRE( c.reset(1) );
  REQUIRE( c.reset(2) );

  auto fileio = Factory::makeFileIo();

  int  buf_size = 64;
  char write_buf[] = "rcPOa12L3nhN5Cgvsa6Jlr3gn58VhazjA6oSpKacLFYqZBEu0khRwbWtEjge3BUA";
  char read_buf[buf_size];
  char null_buf[buf_size];
  memset (read_buf, 0, buf_size);
  memset (null_buf, 0, buf_size);

  GIVEN("An illegally constructed path"){
    std::string path("path");

    THEN("Open, Statfs and FileAttr creation throw with ENODEV"){
      REQUIRE_THROWS_AS(fileio->Open(path.c_str(), 0), LoggingException);
      try{
        fileio->Open(path.c_str(), 0);
      }catch(const LoggingException& le){
        REQUIRE(le.errnum() == ENODEV);
      }

      struct statfs sfs;
      REQUIRE_THROWS_AS(fileio->Statfs(path.c_str(), &sfs), LoggingException);
      try{
        fileio->Statfs(path.c_str(), &sfs);
      }catch(const LoggingException& le){
        REQUIRE(le.errnum() == ENODEV);
      }

      REQUIRE_THROWS_AS(Factory::makeFileAttr(path.c_str()), LoggingException);
      try{
        Factory::makeFileAttr(path.c_str());
      }catch(const LoggingException& le){
        REQUIRE(le.errnum() == ENODEV);
      }
    }
  }

  GIVEN ("A valid path, but no existing file."){
    std::string base_path("kinetic:Cluster1:");
    std::string path(base_path+"filename");

    THEN("All IO operations throw LoggingExceptions"){
      REQUIRE_THROWS_AS(fileio->Read(0,read_buf,buf_size), LoggingException);
      REQUIRE_THROWS_AS(fileio->Write(0,write_buf,buf_size), LoggingException);
      REQUIRE_THROWS_AS(fileio->Truncate(0), LoggingException);
      REQUIRE_THROWS_AS(fileio->Remove(), LoggingException);
      REQUIRE_THROWS_AS(fileio->Sync(), LoggingException);
      REQUIRE_THROWS_AS(fileio->Close(), LoggingException);
    }

    THEN("Attempting to open without create flag fails with ENOENT."){
      try{
        fileio->Open(path.c_str(), 0);
      }catch(const LoggingException& le){
        REQUIRE(le.errnum() == ENOENT);
      }
    }

    THEN("Statfs succeeds"){
      struct statfs sfs;
      try{
        fileio->Statfs(path.c_str(), &sfs);
      }catch(...){}
      usleep(1000 * 1000);
      REQUIRE_NOTHROW(fileio->Statfs(path.c_str(), &sfs));
      REQUIRE(sfs.f_bavail > 0);
    }

    THEN("Factory function for Attribute class returns 0"){
      auto a = Factory::makeFileAttr(path.c_str());
      REQUIRE_FALSE(a);
     }

    THEN("ftsRead returns \"\"."){
      void * handle = fileio->ftsOpen(base_path);
      REQUIRE(handle != NULL);
      REQUIRE(fileio->ftsRead(handle) == "");
      REQUIRE(fileio->ftsClose(handle) == 0);
    }
  }

  GIVEN("A file is created."){
    std::string base_path("kinetic:Cluster2:");
    std::string path(base_path+"filename");

    REQUIRE_NOTHROW(fileio->Open(path.c_str(), SFS_O_CREAT));
    
    THEN("Trying to open anything again on the non-closed object fails with EPERM"){
      try{
        fileio->Open(path.c_str(), SFS_O_CREAT);
      }catch(const LoggingException& le){
        REQUIRE(le.errnum() == EPERM);
      }
    }

    THEN("Trying to create the same file again fails with EEXIST"){
      try{
        Factory::makeFileIo()->Open(path.c_str(), SFS_O_CREAT);
      }catch(const LoggingException& le){
        REQUIRE(le.errnum() == EEXIST);
      }
    }

    WHEN("A buffer is read into memory"){
      int size = 20;
      char abuf[size];
      char bbuf[size];

      int fd = open("/dev/random", O_RDONLY);
      read(fd, abuf, size);
      close(fd);
      abuf[10]=0;

      THEN("We can write it to the filio object & read it straight away."){
          REQUIRE(fileio->Write(0,abuf,size) == size);
          REQUIRE(fileio->Read(0,bbuf,size) == size);
          REQUIRE(memcmp(abuf,bbuf,size) == 0);
          fileio->Close();

          AND_THEN("We can read it in again after reopening the object."){
            REQUIRE_NOTHROW(fileio->Open(path.c_str(), 0));
            REQUIRE(fileio->Read(0,bbuf,size) == size);
            REQUIRE(memcmp(abuf,bbuf,size) == 0);
          }
      }
    }

    THEN("The first ftsRead returns the full path, the second \"\"."){
      fileio->Close();
      void * handle = fileio->ftsOpen(base_path);
      REQUIRE(handle != NULL);
      REQUIRE(fileio->ftsRead(handle) == path);
      REQUIRE(fileio->ftsRead(handle) == "");
      REQUIRE(fileio->ftsClose(handle) == 0);
    }

    THEN("Factory function for Attribute class succeeds"){
      auto a = Factory::makeFileAttr(path.c_str());
      REQUIRE(a);

      AND_THEN("attributes can be set and read-in again."){
        REQUIRE(a->Set("name", write_buf, buf_size) == true);
        size_t size = buf_size;
        REQUIRE(a->Get("name",read_buf,size) == true);
        REQUIRE(size == buf_size);
        REQUIRE(memcmp(write_buf,read_buf,buf_size) == 0);
      
        AND_THEN("attributes are not returned by by ftsRead"){
            fileio->Close();
            void * handle = fileio->ftsOpen(base_path);
            REQUIRE(handle != NULL);
            REQUIRE(fileio->ftsRead(handle) == path);
            REQUIRE(fileio->ftsRead(handle) == "");
            REQUIRE(fileio->ftsClose(handle) == 0);
        }
      }
      
      AND_THEN("We can use the attr interface to request io stats"){
        size_t size = buf_size;
        REQUIRE(a->Get("sys.iostats.read-ops", read_buf, size) == true);
        REQUIRE(a->Get("sys.iostats.read-bw", read_buf, size) == true);
        REQUIRE(a->Get("sys.iostats.write-ops", read_buf, size) == true);
        REQUIRE(a->Get("sys.iostats.write-bw", read_buf, size) == true);
        REQUIRE(a->Get("sys.iostats.max-bw", read_buf, size) == true);
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
      REQUIRE(fileio->Write(77777777,write_buf,buf_size) == buf_size);

      AND_THEN("Written data can be read in again."){
        REQUIRE(fileio->Read(77777777, read_buf, buf_size) == buf_size);
        REQUIRE(memcmp(write_buf,read_buf,buf_size) == 0);
      }

      AND_THEN("Reading with offset < filesize but offset+length > filesize only reads to filesize limits"){
        REQUIRE(fileio->Read(77777777+buf_size/2, read_buf, buf_size) == buf_size/2);
      }

      AND_THEN("Reading data before the offset is possible and returns 0s (file with holes)"){
        REQUIRE(fileio->Read(66666666, read_buf, buf_size) == buf_size);
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
          THEN("stat succeeds and returns the truncated size"){
              struct stat stbuf;
              REQUIRE_NOTHROW(fileio->Stat(&stbuf));
              REQUIRE(stbuf.st_size == size);
          }
        }
      }
    }
    
    AND_WHEN("The file is removed via a second io object."){
      auto fileio_2nd = Factory::makeFileIo();
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

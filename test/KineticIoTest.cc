#include "catch.hpp"
#include "KineticFileIo.hh"
#include "KineticFileAttr.hh"
#include <unistd.h>


SCENARIO("KineticIo Integration Test", "[Io]"){

  kinetic::ConnectionOptions tls_t1 = { "localhost", 8443, true, 1, "asdfasdf" };
  kinetic::ConnectionOptions tls_t2 = { "localhost", 8444, true, 1, "asdfasdf" };

  kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
  std::shared_ptr<kinetic::BlockingKineticConnection> con1;
  std::shared_ptr<kinetic::BlockingKineticConnection> con2;
  REQUIRE(factory.NewBlockingConnection(tls_t1, con1, 30).ok());
  REQUIRE(factory.NewBlockingConnection(tls_t2, con2, 30).ok());
  REQUIRE(con1->InstantErase("NULL").ok());
  REQUIRE(con2->InstantErase("NULL").ok());

  KineticFileIo kio;
  std::string base_path("kinetic:Cluster1:");
  std::string path(base_path+"filename");

  int  buf_size = 64;
  char write_buf[] = "rcPOa12L3nhN5Cgvsa6Jlr3gn58VhazjA6oSpKacLFYqZBEu0khRwbWtEjge3BUA";
  char read_buf[buf_size];
  char null_buf[buf_size];
  memset (read_buf, 0, buf_size);
  memset (null_buf, 0, buf_size);

  GIVEN("An illegally constructed path"){
    std::string path("path");

    THEN("Calling Open returns ENODEV"){
      REQUIRE_THROWS(kio.Open(path.c_str(), 0));
    }

    THEN("Calling Statfs returns ENODEV"){
      struct statfs sfs;
      REQUIRE_THROWS(kio.Statfs(path.c_str(), &sfs));
    }

    THEN("Factory function for Attribute class throws"){
      REQUIRE_THROWS(KineticFileAttr::OpenAttr(path.c_str()));
    }
  }

  GIVEN ("A kio object for a nonexisting file"){

    THEN("All public operations (except Statfs) fail with ENXIO error code"){
      REQUIRE_THROWS(kio.Read(0,read_buf,buf_size));
      REQUIRE_THROWS(kio.Write(0,write_buf,buf_size));
      REQUIRE_THROWS(kio.Truncate(0));
      REQUIRE_THROWS(kio.Remove());
      REQUIRE_THROWS(kio.Sync());
      REQUIRE_THROWS(kio.Close());
    }

    THEN("Statfs succeeds"){
      struct statfs sfs;
      REQUIRE_NOTHROW(kio.Statfs(path.c_str(), &sfs));
      REQUIRE(sfs.f_bavail > 0);
    }

    THEN("Factory function for Attribute class returns 0"){
      std::unique_ptr<KineticFileAttr> a;
      a.reset( KineticFileAttr::OpenAttr(path.c_str()) );
      REQUIRE(!a);
     }

    THEN("ftsRead returns \"\"."){
      void * handle = kio.ftsOpen(base_path);
      REQUIRE(handle != NULL);
      REQUIRE(kio.ftsRead(handle) == "");
      REQUIRE(kio.ftsClose(handle) == 0);
    }
  }

  GIVEN("Open succeeds"){
    REQUIRE_NOTHROW(kio.Open(path.c_str(), 0));

    THEN("The first ftsRead returns the full path, the second \"\"."){
      void * handle = kio.ftsOpen(base_path);
      REQUIRE(handle != NULL);
      REQUIRE(kio.ftsRead(handle) == path);
      REQUIRE(kio.ftsRead(handle) == "");
      REQUIRE(kio.ftsClose(handle) == 0);
    }

    THEN("Factory function for Attribute class succeeds"){
      std::unique_ptr<KineticFileAttr> a;
      a.reset( KineticFileAttr::OpenAttr(path.c_str()) );
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
      REQUIRE(kio.Read(0,read_buf,buf_size) == 0);
    }

    THEN("Attempting to read an empty file with and offset reads 0 bytes."){
      REQUIRE(kio.Read(199,read_buf,buf_size) == 0);
    }

    THEN("Writing is possible from object start."){
      REQUIRE(kio.Write(0, write_buf, buf_size) == buf_size);

      AND_THEN("Written data can be read in again."){
        REQUIRE(kio.Read(0, read_buf, buf_size) == buf_size);
        REQUIRE(memcmp(write_buf,read_buf,buf_size) == 0);
      }
    }

    THEN("Writing is possible from an offset."){
      REQUIRE(kio.Write(1000000,write_buf,buf_size) == buf_size);

      AND_THEN("Written data can be read in again."){
        REQUIRE(kio.Read(1000000, read_buf, buf_size) == buf_size);
        REQUIRE(memcmp(write_buf,read_buf,buf_size) == 0);
      }

      AND_THEN("Reading with offset < filesize but offset+length > filesize only reads to filesize limits"){
        REQUIRE(kio.Read(1000000+buf_size/2, read_buf, buf_size) == buf_size/2);
      }

      AND_THEN("Reading data before the offset is possible and returns 0s (file with holes)"){
        REQUIRE(kio.Read(1000000/3, read_buf, buf_size) == buf_size);
        REQUIRE(memcmp(null_buf,read_buf,buf_size) == 0);
      }
    }

    THEN("Stat should succeed and report a file size of 0"){
      struct stat stbuf;
      REQUIRE_NOTHROW(kio.Stat(&stbuf));
      REQUIRE(stbuf.st_blocks == 1);
      REQUIRE(stbuf.st_blksize == 1024*1024);
      REQUIRE(stbuf.st_size == 0);
    }

    WHEN("Truncate is called to change the file size."){
      for(int chunk=3; chunk>=0; chunk--){
        for(int odd=0; odd<=1; odd++){
          size_t size = 1024*1024*chunk+odd;
          REQUIRE_NOTHROW(kio.Truncate(size));
          THEN("statfs succeeds and returns the truncated size"){
              struct stat stbuf;
              REQUIRE_NOTHROW(kio.Stat(&stbuf));
              REQUIRE(stbuf.st_size == size);
          }
        }
      }
    }

    THEN("Calling statfs on the same object is illegal,"){
      struct statfs sfs;
      REQUIRE_THROWS(kio.Statfs(base_path.c_str(), &sfs));
    }

    AND_WHEN("The file is removed via a second io object."){
      KineticFileIo kio2nd;
      REQUIRE_NOTHROW(kio2nd.Open(path.c_str(), 0));
      REQUIRE_NOTHROW(kio2nd.Remove());

      THEN("The the change will not immediately be visible to the first io object"){
        struct stat stbuf;
        REQUIRE_NOTHROW(kio.Stat(&stbuf));
      }

      THEN("Calling stat will fail with ENOENT after expiration time has run out."){
        usleep(ClusterChunk::expiration_time * 1000);
        struct stat stbuf;
        REQUIRE_THROWS(kio.Stat(&stbuf));
      }
    }

    AND_WHEN("Writing data across multiple chunks."){
      const size_t capacity = 1024*1024;
      REQUIRE(kio.Write(capacity-32, write_buf, buf_size) == buf_size);

      THEN("IO object can be synced."){
        REQUIRE_NOTHROW(kio.Sync());
      }

      THEN("Stat will return the number of blocks and the filesize."){
        struct stat stbuf;
        REQUIRE_NOTHROW(kio.Stat(&stbuf));
        REQUIRE(stbuf.st_blocks == 2);
        REQUIRE(stbuf.st_blksize == capacity);
        REQUIRE(stbuf.st_size == stbuf.st_blksize-32+buf_size);
      }

      THEN("The file can can be removed again."){
        REQUIRE_NOTHROW(kio.Remove());
        REQUIRE_NOTHROW(kio.Close());

        AND_THEN("ftsRead returns \"\"."){
          void * handle = kio.ftsOpen(base_path);
          REQUIRE(handle != NULL);
          REQUIRE(kio.ftsRead(handle) == "");
          REQUIRE(kio.ftsClose(handle) == 0);
        }
      }
    }
  }
}

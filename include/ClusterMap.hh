//------------------------------------------------------------------------------
//! @file ClusterMap.hh
//! @author Paul Hermann Lensing
//! @brief Providing access to cluster instances and the data io cache.
//------------------------------------------------------------------------------
#ifndef KINETICDRIVEMAP_HH
#define KINETICDRIVEMAP_HH

/*----------------------------------------------------------------------------*/
#include <condition_variable>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <mutex>
#include <json-c/json.h>
#include "ClusterInterface.hh"
#include "ErasureCoding.hh"
#include "LRUCache.hh"
#include "SocketListener.hh"
#include "ClusterChunkCache.hh"
#include "KineticAdminCluster.hh"
/*----------------------------------------------------------------------------*/

namespace kio {

//------------------------------------------------------------------------------
//! Providing access to cluster instances and the data io cache. Threadsafe.
//------------------------------------------------------------------------------
class ClusterMap
{
public:
  //--------------------------------------------------------------------------
  //! Obtain an input-output class for the supplied identifier.
  //!
  //! @param id the unique identifier for the cluster
  //! @return a valid cluster object
  //--------------------------------------------------------------------------
  std::shared_ptr<ClusterInterface> getCluster(const std::string& id);

  //--------------------------------------------------------------------------
  //! Obtain an admin cluster instance for the supplied identifier.
  //!
  //! @param id the unique identifier for the cluster
  //! @param numthreads number of background io threads during scan operations
  //! @return a valid admin cluster object
  //--------------------------------------------------------------------------
  std::unique_ptr<KineticAdminCluster> getAdminCluster(const std::string& id, size_t numthreads);

  //--------------------------------------------------------------------------
  //! Obtain a reference to the io data cache.
  //!
  //! @return a reference to the data cache
  //--------------------------------------------------------------------------
  ClusterChunkCache& getCache();

  //--------------------------------------------------------------------------
  //! (Re)load the json configuration files and reconfigure the ClusterMap
  //! accordingly.
  //--------------------------------------------------------------------------
  void loadConfiguration();

  //--------------------------------------------------------------------------
  //! ClusterMap is shared among all FileIo objects.
  //--------------------------------------------------------------------------
  static ClusterMap& getInstance() {
    static ClusterMap clustermap;
    return clustermap;
  }

  //--------------------------------------------------------------------------
  //! Copy constructing makes no sense
  //--------------------------------------------------------------------------
  ClusterMap(ClusterMap&) = delete;

  //--------------------------------------------------------------------------
  //! Assignment make no sense
  //--------------------------------------------------------------------------
  void operator=(ClusterMap&) = delete;


private:
    //--------------------------------------------------------------------------
    //! Configuration of library wide parameters.
    //--------------------------------------------------------------------------
    struct Configuration{
        //! the preferred size in bytes of the data cache
        size_t stripecache_target;
        //! the absolut maximum size of the data cache in bytes
        size_t stripecache_capacity;
        //! the maximum number of keys prefetched by readahead algorithm
        size_t readahead_window_size;
        //! the number of threads used for bg io in the data cache
        int background_io_threads;
        //! the maximum number of operations queued for bg io
        int background_io_queue_capacity;
        //! the number of erasure coding instances that may be cached
        int num_erasure_codings;
        //! the number of coding tables each erasure coding instance may cache
        int num_erasure_coding_tables;
    };

    //! storing the library wide configuration parameters
    Configuration configuration;

    //--------------------------------------------------------------------------
    //! Store a cluster object and all information required to create it
    //--------------------------------------------------------------------------
    struct KineticClusterInfo
    {
        //! the number of data blocks in a stripe
        std::size_t numData;
        //! the number of parity blocks in a stripe
        std::size_t numParity;
        //! the size of a single data / parity block in bytes
        std::size_t blockSize;
        //! minimum interval between reconnection attempts to a drive (rate limit)
        std::chrono::seconds min_reconnect_interval;
        //! interval after which an operation will timeout without response
        std::chrono::seconds operation_timeout;
        //! the unique ids of drives belonging to this cluster
        std::vector<std::string> drives;
        //! the cluster object, shared among IO objects of a fst
        std::shared_ptr<ClusterInterface> cluster;
    };

    //! the cluster map id <-> cluster info
    std::unordered_map<std::string, KineticClusterInfo> clustermap;

    //! the drive map id <-> connection info
    std::unordered_map<std::string, std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>> drivemap;

    //! ErasureCcoding instances of the same type (nData,nParity) can be shared
    //! among multiple cluster instances, no need to duplicate decoding tables
    //! in memory.
    std::unique_ptr<LRUCache<std::string, std::shared_ptr<ErasureCoding>>> ecCache;

    //! the data cache shared among cluster instances
    std::unique_ptr<ClusterChunkCache> dataCache;

    //! epoll listener loop shared among all connections
    std::unique_ptr<SocketListener> listener;

    //! concurrency control
    std::mutex mutex;

private:
  //--------------------------------------------------------------------------
  //! Constructor.
  //! Requires a json file listing kinetic drives to be stored at the location
  //! indicated by the KINETIC_DRIVE_LOCATION and KINETIC_DRIVE_SECURITY
  //! environment variables
  //--------------------------------------------------------------------------
  explicit ClusterMap();

  //--------------------------------------------------------------------------
  //! Set erasure coding instance and connection options based on the
  //! inforamtion available in the supplied KineticClusterInfo.
  //!
  //! @param ki Cluster Information
  //! @param ec Erase Coding instance to set
  //! @param cops Connection Options to fill
  //--------------------------------------------------------------------------
  void fillArgs(const KineticClusterInfo &ki,
                std::shared_ptr<ErasureCoding>& ec,
                std::vector<std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>>& cops
  );

  //--------------------------------------------------------------------------
  //! Private enum to differentiate between json configuration files.
  //--------------------------------------------------------------------------
  enum class filetype { location, security, cluster };

  //--------------------------------------------------------------------------
  //! Parse the supplied json file.
  //!
  //! @param filedata contents of a json file
  //! @param filetype specifies if filedata contains security or location
  //!        information.
  //--------------------------------------------------------------------------
  void parseJson(const std::string& filedata, filetype type);

  //--------------------------------------------------------------------------
  //! Creates a KineticConnection pair in the drive map containing the ip and
  //! port information.
  //!
  //! @param drive json root of one drive description containing location data
  //--------------------------------------------------------------------------
  void parseDriveLocation(struct json_object* drive);

  //--------------------------------------------------------------------------
  //! Adds security attributes to drive description
  //!
  //! @param drive json root of one drive description containing security data
  //--------------------------------------------------------------------------
  void parseDriveSecurity(struct json_object* drive);

  //--------------------------------------------------------------------------
  //! Adds security attributes to drive description
  //!
  //! @param drive json root of one drive description containing security data
  //--------------------------------------------------------------------------
  void parseClusterInformation(struct json_object* cluster);

  //--------------------------------------------------------------------------
  //! Adds security attributes to drive description
  //!
  //! @param drive json root of library configuration
  //--------------------------------------------------------------------------
  void parseConfiguration(struct json_object* configuration);

};

}


#endif	/* KINETICDRIVEMAP_HH */


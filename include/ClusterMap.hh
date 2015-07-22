//------------------------------------------------------------------------------
//! @file ClusterMap.hh
//! @author Paul Hermann Lensing
//! @brief Supplying a fst wide cluster map. Threadsafe.
//------------------------------------------------------------------------------
#ifndef KINETICDRIVEMAP_HH
#define  KINETICDRIVEMAP_HH

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
/*----------------------------------------------------------------------------*/

namespace kio {

//------------------------------------------------------------------------------
//! Supplying a fst wide cluster map. Threadsafe.
//------------------------------------------------------------------------------
class ClusterMap
{

public:
  //--------------------------------------------------------------------------
  //! Obtain an input-output class for the supplied identifier.
  //!
  //! @param id the unique identifier for the cluster
  //! @param cluster contains the cluster on success
  //--------------------------------------------------------------------------
  std::shared_ptr<ClusterInterface> getCluster(const std::string& id);

  //--------------------------------------------------------------------------
  //! Obtain the number of entries in the map.
  //!
  //! @return the number of entries in the map
  //--------------------------------------------------------------------------
  size_t getSize();

  //--------------------------------------------------------------------------
  //! ClusterMap is shared among all FileIo objects.
  //--------------------------------------------------------------------------
  static ClusterMap& getInstance() {
    static ClusterMap clustermap;
    return clustermap;
  }

private:
  //--------------------------------------------------------------------------
  //! Constructor.
  //! Requires a json file listing kinetic drives to be stored at the location
  //! indicated by the KINETIC_DRIVE_LOCATION and KINETIC_DRIVE_SECURITY
  //! environment variables
  //--------------------------------------------------------------------------
  explicit ClusterMap();

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~ClusterMap();

  //--------------------------------------------------------------------------
  //! Copy constructing makes no sense
  //--------------------------------------------------------------------------
  ClusterMap(ClusterMap&) = delete;

  //--------------------------------------------------------------------------
  //! Assignment make no sense
  //--------------------------------------------------------------------------
  void operator=(ClusterMap&) = delete;

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
  //! @return 0 if successful, EINVAL if drive description incomplete or
  //!         incorrect json.
  //--------------------------------------------------------------------------
  int parseJson(const std::string& filedata, filetype type);

  //--------------------------------------------------------------------------
  //! Creates a KineticConnection pair in the drive map containing the ip and
  //! port information.
  //!
  //! @param drive json root of one drive description containing location data
  //! @return 0 if successful, EINVAL if name entry not available
  //--------------------------------------------------------------------------
  int parseDriveLocation(struct json_object* drive);

  //--------------------------------------------------------------------------
  //! Adds security attributes to drive description
  //!
  //! @param drive json root of one drive description containing security data
  //! @return 0 if successful, EINVAL if drive description incomplete or
  //!         incorrect json,  ENODEV if drive id does not exist in map.
  //--------------------------------------------------------------------------
  int parseDriveSecurity(struct json_object* drive);

  //--------------------------------------------------------------------------
  //! Adds security attributes to drive description
  //!
  //! @param drive json root of one drive description containing security data
  //! @return 0 if successful, EINVAL if drive description incomplete or
  //!         incorrect json,  ENODEV if drive id does not exist in map.
  //--------------------------------------------------------------------------
  int parseClusterInformation(struct json_object* cluster);

private:
  //--------------------------------------------------------------------------
  //! Store a cluster object and all information required to create it
  //--------------------------------------------------------------------------
  struct KineticClusterInfo
  {
    //! the number of data blocks in a stripe
    std::size_t numData;
    //! the number of parity blocks in a stripe
    std::size_t numParity;
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
  std::unordered_map<std::string, std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions> > drivemap;

  //! ErasureEncoding instances of the same type (nData,nParity) can be shared
  //! among multiple cluster instances, no need to duplicate decoding tables
  //! in memory.
  cache::lru_cache<std::string, std::shared_ptr<ErasureCoding>> ecCache;

  //! epoll listener loop shared among all connections
  std::unique_ptr<SocketListener> listener;

  //! concurrency control
  std::mutex mutex;
};

}


#endif	/* KINETICDRIVEMAP_HH */


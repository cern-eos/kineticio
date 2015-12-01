//------------------------------------------------------------------------------
//! @file KineticIoSingleton.hh
//! @author Paul Hermann Lensing
//! @brief Providing access to global library structures
//------------------------------------------------------------------------------
#ifndef KINETICIO_SINGLETON_HH
#define KINETICIO_SINGLETON_HH

/*----------------------------------------------------------------------------*/
#include "ClusterMap.hh"
#include "DataCache.hh"
#include "BackgroundOperationHandler.hh"
/*----------------------------------------------------------------------------*/

namespace kio {

class KineticIoSingleton {

public:
  //! return cluster map 
  ClusterMap& cmap();
  
  //! return cache 
  DataCache& cache(); 
  
  //! return thread pool 
  BackgroundOperationHandler& threadpool();
  
  //--------------------------------------------------------------------------
  //! (Re)load the json configuration files and reconfigure the ClusterMap
  //! accordingly.
  //--------------------------------------------------------------------------
  void loadConfiguration();
  
public:
  //--------------------------------------------------------------------------
  //! Superblock is globally shared
  //--------------------------------------------------------------------------
  static KineticIoSingleton& getInstance();

  //--------------------------------------------------------------------------
  //! Copy constructing makes no sense
  //--------------------------------------------------------------------------
  KineticIoSingleton(KineticIoSingleton&) = delete;

  //--------------------------------------------------------------------------
  //! Assignment make no sense
  //--------------------------------------------------------------------------
  void operator=(KineticIoSingleton&) = delete;

private:
  //--------------------------------------------------------------------------
  //! Configuration of library wide parameters.
  //--------------------------------------------------------------------------
  struct Configuration{
      //! the maximum size of the data cache in bytes
      size_t stripecache_capacity;
      //! the maximum number of keys prefetched by readahead algorithm
      size_t readahead_window_size;
      //! the number of threads used for bg io in the data cache, can be 0
      int background_io_threads;
      //! the maximum number of operations queued for bg io, can be 0 
      int background_io_queue_capacity;
  };

  //! storing the library wide configuration parameters
  Configuration configuration;
  
  //! the cluster map 
  std::unique_ptr<ClusterMap> clusterMap;
  
  //! the data cache shared among cluster instances
  std::unique_ptr<DataCache> dataCache;
  
  //! concurrency control
  std::mutex mutex;
  
private:
  //--------------------------------------------------------------------------
  //! Constructor.
  //! Requires a json configuration in environment variables
  //! KINETIC_DRIVE_LOCATION, KINETIC_DRIVE_SECURITY and 
  //! KINETIC_CLUSTER_DEFINTION. Environment variables can store configuration
  //! directly or contain the path to the respective json file(s). 
  //--------------------------------------------------------------------------
  explicit KineticIoSingleton();
 
  //--------------------------------------------------------------------------
  //! Adds security attributes to drive description
  //!
  //! @param locations json list of drive locations 
  //! @param security json list of drive security configurations
  //--------------------------------------------------------------------------
  std::unordered_map<std::string, std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions>> parseDrives(
          struct json_object* locations, struct json_object* security
  );

  //--------------------------------------------------------------------------
  //! Adds security attributes to drive description
  //!
  //! @param clusters json list of cluster descriptions 
  //--------------------------------------------------------------------------
  std::unordered_map<std::string, ClusterInformation> parseClusters(struct json_object* clusters);

  //--------------------------------------------------------------------------
  //! Adds security attributes to drive description
  //!
  //! @param drive json library configuration
  //--------------------------------------------------------------------------
  Configuration parseConfiguration(struct json_object* configuration);
  
};

static KineticIoSingleton& kio()
{
   return KineticIoSingleton::getInstance();
}

}

#endif


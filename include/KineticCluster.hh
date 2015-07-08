//------------------------------------------------------------------------------
//! @file KineticCluster.hh
//! @author Paul Hermann Lensing
//! @brief General purpose implementation of cluster interface for Kinetic. 
//------------------------------------------------------------------------------
#ifndef KINETICCLUSTER_HH
#define	KINETICCLUSTER_HH

#include "ClusterInterface.hh"
#include "KineticAutoConnection.hh"
#include "KineticAsyncOperation.hh"
#include "ErasureCoding.hh"
#include <chrono>
#include <mutex>

/* <cstdatomic> is part of gcc 4.4.x experimental C++0x support... <atomic> is
 * what actually made it into the standard.
 */
//#if __GNUC__ == 4 && (__GNUC_MINOR__ == 4)
//    #include <cstdatomic>
//#else
//    #include <atomic>
//#endif


namespace kio{

//------------------------------------------------------------------------------
//! Implementation of cluster interface for arbitrarily sized cluster & stripe
//! sizes.
//------------------------------------------------------------------------------
class KineticCluster : public ClusterInterface {
public:
  //! See documentation in superclass.
  const ClusterLimits& limits() const;
  //! See documentation in superclass.
  kinetic::KineticStatus size(
      ClusterSize& size
  );
  //! See documentation in superclass.
  kinetic::KineticStatus get(
      const std::shared_ptr<const std::string>& key,
      bool skip_value,
      std::shared_ptr<const std::string>& version,
      std::shared_ptr<const std::string>& value
  );
  //! See documentation in superclass.
  kinetic::KineticStatus  put(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version,
      const std::shared_ptr<const std::string>& value,
      bool force,
      std::shared_ptr<const std::string>& version_out
  );
  //! See documentation in superclass.
  kinetic::KineticStatus remove(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version,
      bool force
  );
  //! See documentation in superclass.
  kinetic::KineticStatus range(
      const std::shared_ptr<const std::string>& start_key,
      const std::shared_ptr<const std::string>& end_key,
      int maxRequested,
      std::unique_ptr< std::vector<std::string> >& keys
  );

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param num_data the number of data chunks generated for a single value 
  //! @param num_parities the number of parities to be computed for a stripe
  //! @param info host / port / key of target kinetic drives
  //! @param min_reconnect_interval minimum time between reconnection attempts
  //! @param operation_timeout the maximum interval an operation is allowed
  //! @param erasure pointer to an ErasureCoding object
  //--------------------------------------------------------------------------
  explicit KineticCluster(
    std::size_t num_data, std::size_t num_parities,
    std::vector< std::pair < kinetic::ConnectionOptions, kinetic::ConnectionOptions > > info,
    std::chrono::seconds min_reconnect_interval,
    std::chrono::seconds operation_timeout,
    std::shared_ptr<ErasureCoding> erasure
  );

  //--------------------------------------------------------------------------
  //! Destructor.
  //--------------------------------------------------------------------------
  ~KineticCluster();

private:
  //--------------------------------------------------------------------------
  //! Create a set of AsyncOperations and assign target drives / connections.
  //!
  //! @param key the key used to assign connections to AsyncOperations
  //! @param size the number of KineticAsyncOperations to initialize
  //! @return vector of KineticAsyncOperations of size size with
  //!         assigned connections
  //--------------------------------------------------------------------------
  std::vector<KineticAsyncOperation> initialize(
      const std::shared_ptr<const std::string>& key,
      std::size_t size
  );

  //--------------------------------------------------------------------------
  //! Execute the supplied operation, making sure the connection state is valid
  //! before attempting to execute. This function can be thought as as a
  //! dispatcher.
  //!
  //! @param operations the operations that need to be executed
  //! @return status of operation
  //--------------------------------------------------------------------------
  kinetic::KineticStatus execute(
      std::vector< KineticAsyncOperation >& operations
  );

  //--------------------------------------------------------------------------
  //! Update the clustersize / clusterlimits variables. This function is
  //! thread-safe and can be called in the background.
  //!
  //! @param types the log types to request from the backend.
  //! @return status of operation
  //--------------------------------------------------------------------------
  kinetic::KineticStatus getLog(
      std::vector<kinetic::Command_GetLog_Type> types
  );

  //--------------------------------------------------------------------------
  //! Get the version of the supplied key. This function will be called by
  //! get() if skip_value is set to true.
  //!
  //! @param key the key
  //! @param version stores the version upon success, not modified on error
  //! @return status of operation
  //--------------------------------------------------------------------------
  kinetic::KineticStatus getVersion(
      const std::shared_ptr<const std::string>& key,
      std::shared_ptr<const std::string>& version
  );

private:
  //! number of data chunks in a stripe
  std::size_t nData;

  //! number of parities in a stripe
  std::size_t nParity;

  //! all connections associated with this cluster
  std::vector< KineticAutoConnection > connections;

  //! timeout of asynchronous operations
  std::chrono::seconds operation_timeout;

  //! cluster limits are constant over cluster lifetime
  ClusterLimits clusterlimits;

  //! size information of the cluster (total / free bytes)
  ClusterSize clustersize;

  //! status of last attempt to update the drive log
  kinetic::KineticStatus getlog_status;

  //! true if there is an outstanding background thread
  bool getlog_outstanding;

  //! concurrency control
  std::mutex getlog_mutex;

  //! erasure coding
  std::shared_ptr<ErasureCoding> erasure;
};

}

#endif	/* KINETICCLUSTER_HH */

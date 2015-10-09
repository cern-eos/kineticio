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
#include "KineticCallbacks.hh"
#include "SocketListener.hh"
#include "ErasureCoding.hh"
#include <utility>
#include <chrono>
#include <mutex>

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
  ClusterSize size();
  //! See documentation in superclass.
  ClusterIo iostats(); 
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
  //! @param block_size the size of a single data / parity block in bytes
  //! @param targets host / port / key of target kinetic drives
  //! @param min_reconnect_interval minimum time between reconnection attempts
  //! @param operation_timeout the maximum interval an operation is allowed
  //! @param erasure pointer to an ErasureCoding object
  //--------------------------------------------------------------------------
  explicit KineticCluster(
    std::size_t num_data, std::size_t num_parities, std::size_t block_size,
    std::vector< std::pair < kinetic::ConnectionOptions, kinetic::ConnectionOptions > > targets,
    std::chrono::seconds min_reconnect_interval,
    std::chrono::seconds operation_timeout,
    std::shared_ptr<ErasureCoding> erasure,
    SocketListener& sockwatch
  );

  //--------------------------------------------------------------------------
  //! Destructor.
  //--------------------------------------------------------------------------
  virtual ~KineticCluster();

protected:
  //--------------------------------------------------------------------------
  //! Create a set of AsyncOperations and assign target drives / connections.
  //!
  //! @param key the key used to assign connections to AsyncOperations
  //! @param size the number of KineticAsyncOperations to initialize
  //! @param offset start vector at offset drive
  //! @return vector of KineticAsyncOperations of size size with
  //!         assigned connections
  //--------------------------------------------------------------------------
  std::vector<KineticAsyncOperation> initialize(
      std::shared_ptr<const std::string> key,
      std::size_t size, off_t offset=0
  );

  //--------------------------------------------------------------------------
  //! Compare kinetic::StatusCode, always evaluating regular results smaller
  //! than error codes. Needed in any case, as gcc 4.4 cannot automatically
  //! compare enum class instances.
  //--------------------------------------------------------------------------
  struct compareStatusCode{
    bool operator()(const kinetic::StatusCode& lhs, const kinetic::StatusCode& rhs) const
    {
      int l = static_cast<int>(lhs);
      if( lhs != kinetic::StatusCode::OK &&
          lhs != kinetic::StatusCode::REMOTE_NOT_FOUND &&
          lhs != kinetic::StatusCode::REMOTE_VERSION_MISMATCH)
        l+=100;

      int r = static_cast<int>(rhs);
      if( rhs != kinetic::StatusCode::OK &&
          rhs != kinetic::StatusCode::REMOTE_NOT_FOUND &&
          rhs != kinetic::StatusCode::REMOTE_VERSION_MISMATCH)
        r+=100;

      return l < r;
    }
  };

  //--------------------------------------------------------------------------
  //! Execute the supplied operation, making sure the connection state is valid
  //! before attempting to execute. This function can be thought as as a
  //! dispatcher.
  //!
  //! @param operations the operations that need to be executed
  //! @param sync provide wait_until functionality for asynchronous operations
  //! @return status of operations
  //--------------------------------------------------------------------------
  std::map<kinetic::StatusCode, int, compareStatusCode> execute(
      std::vector<KineticAsyncOperation>& operations,
      CallbackSynchronization& sync
  );

  //--------------------------------------------------------------------------
  //! Concurrency resolution: In case of partial stripe writes / removes due
  //! to concurrent write accesses, decide which client wins the race based
  //! on achieved write pattern and using remote versions as a tie braker.
  //!
  //! @param key the key of the stripe
  //! @param version the version the stripe subchunks should have, empty
  //!   signifies deleted.
  //! @param rmap the results of the partial put / delete operation causing
  //!   a call to mayForce.
  //! @return true if client may force-overwrite subchunks that have a
  //!   different version, false otherwise
  //--------------------------------------------------------------------------
  bool mayForce(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version,
      std::map<kinetic::StatusCode, int, compareStatusCode> rmap
  );

  //--------------------------------------------------------------------------
  //! Update the clusterio statistics and capacity information.
  //--------------------------------------------------------------------------
  void updateStatistics();
  
  //--------------------------------------------------------------------------
  //! In case a get operation notices missing / corrupt / inaccessible chunks,
  //! it may put an indicator key, so that the affected stripe may be looked 
  //! at by a repair process. This will be done asynchronously. 
  //! 
  //! @param key the key of the affected stripe
  //! @param ops the operation vector containing the failed operation 
  //--------------------------------------------------------------------------
  void putIndicatorKey(const std::shared_ptr<const std::string>& key, const std::vector<KineticAsyncOperation>& ops);


protected:
  //! number of data chunks in a stripe
  const std::size_t nData;

  //! number of parities in a stripe
  const std::size_t nParity;

  //! timeout of asynchronous operations
  const std::chrono::seconds operation_timeout;
  
  //! all connections associated with this cluster
  std::vector< std::unique_ptr<KineticAutoConnection> > connections;

  //! time point we last required parity information during a get operation, can be used to enable / disable
  //! reading parities with the data
  std::chrono::system_clock::time_point parity_required;

  //! time point the clusterio statistics have last been updated, used to compute per second values
  std::chrono::system_clock::time_point statistics_timepoint; 

  //! the io statistics at time point timep_statistics 
  ClusterIo statistics_snapshot; 
  
  //! io statistics as per second value of cluster
  ClusterIo clusterio;
  
  //! size information of the cluster (total / free bytes)
  ClusterSize clustersize;

  //! cluster limits are constant over cluster lifetime
  ClusterLimits clusterlimits;
  
  //! updating cluster size in the background
  BackgroundOperationHandler background;

  //! concurrency control of cluster io, size and time_point variables
  std::mutex mutex;

  //! erasure coding
  std::shared_ptr<ErasureCoding> erasure;
};

}

#endif	/* KINETICCLUSTER_HH */

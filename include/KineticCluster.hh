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
  //! @param targets host / port / key of target kinetic drives
  //! @param min_reconnect_interval minimum time between reconnection attempts
  //! @param operation_timeout the maximum interval an operation is allowed
  //! @param erasure pointer to an ErasureCoding object
  //--------------------------------------------------------------------------
  explicit KineticCluster(
    std::size_t num_data, std::size_t num_parities,
    std::vector< std::pair < kinetic::ConnectionOptions, kinetic::ConnectionOptions > > targets,
    std::chrono::seconds min_reconnect_interval,
    std::chrono::seconds operation_timeout,
    std::shared_ptr<ErasureCoding> erasure,
    SocketListener& sockwatch
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
  //! @param offset start vector at offset drive
  //! @return vector of KineticAsyncOperations of size size with
  //!         assigned connections
  //--------------------------------------------------------------------------

  std::vector<KineticAsyncOperation> initialize(
      std::shared_ptr<const std::string> key,
      std::size_t size, off_t offset=0
  );

  //! gcc 4.4 needs some help to compare enum class instances, so they can be
  //! used in a std::map
  struct compareStatusCode{
    bool operator()(const kinetic::StatusCode& lhs, const kinetic::StatusCode& rhs) const
    {



      int l = static_cast<int>(lhs);
      if(lhs != kinetic::StatusCode::OK && lhs != kinetic::StatusCode::REMOTE_NOT_FOUND && lhs != kinetic::StatusCode::REMOTE_VERSION_MISMATCH)
        l+=100;
      int r = static_cast<int>(rhs);
      if(rhs != kinetic::StatusCode::OK && rhs != kinetic::StatusCode::REMOTE_NOT_FOUND && rhs != kinetic::StatusCode::REMOTE_VERSION_MISMATCH)
        r+=100;

      bool rtn = l < r;
      return rtn;
     // return static_cast<int>(lhs) < static_cast<int>(rhs);
    }
  };

  //--------------------------------------------------------------------------
  //! Execute the supplied operation, making sure the connection state is valid
  //! before attempting to execute. This function can be thought as as a
  //! dispatcher.
  //!
  //! @param operations the operations that need to be executed
  //! @return status of operations
  //--------------------------------------------------------------------------
  std::map<kinetic::StatusCode, int, compareStatusCode> execute(
      std::vector<KineticAsyncOperation>& operations,
      CallbackSynchronization& sync
  );

  bool mayForce(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version,
      std::map<kinetic::StatusCode, int, compareStatusCode> rmap
  );

  //--------------------------------------------------------------------------
  //! Update the clustersize variable.
  //!
  //! @param types the log types to request from the backend.
  //! @return status of operation
  //--------------------------------------------------------------------------
  void updateSize();

private:
  //! number of data chunks in a stripe
  std::size_t nData;

  //! number of parities in a stripe
  std::size_t nParity;

  //! all connections associated with this cluster
  std::vector< std::unique_ptr<KineticAutoConnection> > connections;

  //! timeout of asynchronous operations
  std::chrono::seconds operation_timeout;

  //! cluster limits are constant over cluster lifetime
  ClusterLimits clusterlimits;

  //! size information of the cluster (total / free bytes)
  ClusterSize clustersize;

  //! status of last attempt to update the drive log
  kinetic::KineticStatus sizeStatus;

  //! updating cluster size in the background
  BackgroundOperationHandler bg;

  //! concurrency control
  std::mutex mutex;

  //! erasure coding
  std::shared_ptr<ErasureCoding> erasure;
};

}

#endif	/* KINETICCLUSTER_HH */

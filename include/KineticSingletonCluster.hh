#ifndef KINETICSPACESIMPLECPP_HH
#define	KINETICSPACESIMPLECPP_HH

#include "ClusterInterface.hh"
#include <chrono>
#include <mutex>

namespace kio{

/* Implementing the interface for a single drive. */
class KineticSingletonCluster : public ClusterInterface {
public:
  //! See documentation in superclass.
  const ClusterLimits& limits() const;
  //! See documentation in superclass.
  kinetic::KineticStatus size(ClusterSize& size);
  //! See documentation in superclass.
  kinetic::KineticStatus get(const std::shared_ptr<const std::string>& key,
      bool skip_value,
      std::shared_ptr<const std::string>& version,
      std::shared_ptr<const std::string>& value);
  //! See documentation in superclass.
  kinetic::KineticStatus  put(
    const std::shared_ptr<const std::string>& key,
    const std::shared_ptr<const std::string>& version,
    const std::shared_ptr<const std::string>& value,
    bool force,
    std::shared_ptr<const std::string>& version_out);
  //! See documentation in superclass.
  kinetic::KineticStatus remove(const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version,
      bool force);
  //! See documentation in superclass.
  kinetic::KineticStatus range(
      const std::shared_ptr<const std::string>& start_key,
      const std::shared_ptr<const std::string>& end_key,
      int maxRequested,
      std::unique_ptr< std::vector<std::string> >& keys);

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param connection_info host / port / key of target kinetic drive
  //! @param min_reconnect_interval minimum time between reconnection attempts
  //! @param min_getlog_interval minimum time between getlog attempts
  //--------------------------------------------------------------------------
  explicit KineticSingletonCluster(
    const kinetic::ConnectionOptions &con_info,
    std::chrono::seconds min_reconnect_interval,
    std::chrono::seconds min_getlog_interval);

  //--------------------------------------------------------------------------
  //! Destructor.
  //--------------------------------------------------------------------------
  ~KineticSingletonCluster();

private:
  //--------------------------------------------------------------------------
  //! Attempt to build a connection to a kinetic drive using the connection
  //! information that has been supplied to the constructor.
  //--------------------------------------------------------------------------
  void connect();

  //--------------------------------------------------------------------------
  //! Attempt to get the log from the currently connected drive.
  //!
  //! @param types requested to be updated from the drive.
  //! @return status of operation
  //--------------------------------------------------------------------------
  kinetic::KineticStatus getLog(std::vector<kinetic::Command_GetLog_Type> types);

  //--------------------------------------------------------------------------
  //! Execute the supplied operation, making sure the connection state is valid
  //! before attempting to execute and, in case of failure, retry the operation.
  //! This function can be thought as as a dispatcher.
  //!
  //! @param fun the operation that needs to be executed.
  //! @return status of operation
  //--------------------------------------------------------------------------
  kinetic::KineticStatus execute(std::function<kinetic::KineticStatus(void)> f);

private:
  //! connection to a kinetic target
  std::unique_ptr<kinetic::ThreadsafeBlockingKineticConnection> con;

  //! information required to build a connection
  kinetic::ConnectionOptions connection_info;

  //! concurrency control
  std::mutex mutex;

  //! cluster limits are constant over cluster lifetime
  ClusterLimits clusterlimits;

  //! size of the cluster
  ClusterSize clustersize;

  //! timestamp of the last attempt to update cluster size / capacity
  std::chrono::system_clock::time_point getlog_timestamp;
  //! minimum time between attempting to perform getlog operation
  std::chrono::seconds getlog_ratelimit;
  //! status of last attempt to update the drive log
  kinetic::KineticStatus getlog_status;

  //! timestamp of the last connection attempt
  std::chrono::system_clock::time_point connection_timestamp;
  //! minimum time between reconnection attempts
  std::chrono::seconds  connection_ratelimit;
  //! status of last reconnect attempt
  kinetic::KineticStatus connection_status;
};

}


#endif	/* KINETICSPACESIMPLECPP_HH */


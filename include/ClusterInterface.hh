//------------------------------------------------------------------------------
//! @file ClusterInterface.hh
//! @author Paul Hermann Lensing
//! @brief Interface to a cluster...
//------------------------------------------------------------------------------
#ifndef CLUSTERINTERFACE_HH
#define	CLUSTERINTERFACE_HH

#include <vector>
#include <string>
#include <memory>
#include <functional>
#include "kinetic/kinetic.h"

/* TODO: Do not use kinetic::KineticStatus directly, as it can't
 * represent cluster error states. Would also allow to decouple ClusterInterface
 * from kinetic/kinetic.h
 *
enum class KineticClusterStatus{
  OK,
  VERSION_MISSMATCH,
  NOT_FOUND,
  NOT_AUTHORIZED,
  NO_SPACE,
  SERVICE_BUSY,
  CLUSTER_OFFLINE
};
 */

namespace kio{

struct ClusterSize{
  uint64_t bytes_total;
  uint64_t bytes_free;
};

struct ClusterLimits{
  uint32_t max_key_size;
  uint32_t max_version_size;
  uint32_t max_value_size;
};

//------------------------------------------------------------------------------
//! Interface to a cluster, primarily intended to interface with Kinetic
//! drives.
//------------------------------------------------------------------------------
class ClusterInterface{
public:

  //----------------------------------------------------------------------------
  //! Obtain maximum key / version / value sizes.
  //! These limits may drastically differ from standard Kinetic drive limits.
  //! For example, a value might be split and written to multiple drives. Or
  //! some of the key-space might be reserved for cluster internal metadata.
  //! Limits remain constant during the cluster lifetime.
  //!
  //! @return cluster limits
  //----------------------------------------------------------------------------
  virtual const ClusterLimits& limits() const = 0;

  //----------------------------------------------------------------------------
  //! Check the maximum size of the Cluster. Function will not block but
  //! might return outdated values.
  //!
  //! @return clsuter size
  //----------------------------------------------------------------------------
  virtual ClusterSize size() = 0;

  //----------------------------------------------------------------------------
  //! Get the value and version associated with the supplied key.
  //
  //! @param key the key
  //! @param skip_value doesn't request the value from the backend if set.
  //! @param version stores the version upon success, not modified on error
  //! @param value stores the value upon success, not modified on error
  //! @return status of operation
  //----------------------------------------------------------------------------
  virtual kinetic::KineticStatus get(
    const std::shared_ptr<const std::string>& key,
    bool skip_value,
    std::shared_ptr<const std::string>& version,
    std::shared_ptr<const std::string>& value) = 0;

  //----------------------------------------------------------------------------
  //! Write the supplied key-value pair to the Kinetic cluster.
  //!
  //! @param key the key
  //! @param version existing version expected in the cluster, empty for none
  //! @param value value to store
  //! @param force if set, possibly existing version in the cluster will be
  //!   overwritten without checking if supplied version is correct
  //! @param version_out stores cluster generated new version upon success
  //! @return status of operation
  //----------------------------------------------------------------------------
  virtual kinetic::KineticStatus put(
    const std::shared_ptr<const std::string>& key,
    const std::shared_ptr<const std::string>& version,
    const std::shared_ptr<const std::string>& value,
    bool force,
    std::shared_ptr<const std::string>& version_out) = 0;

  //----------------------------------------------------------------------------
  //! Delete the key on the cluster.
  //!
  //! @param key     the key
  //! @param version existing version expected in the cluster
  //! @param force   if set to true, possibly existing version in the cluster
  //!   will not be verified against supplied version.
  //! @return status of operation
   //---------------------------------------------------------------------------
  virtual kinetic::KineticStatus remove(
    const std::shared_ptr<const std::string>& key,
    const std::shared_ptr<const std::string>& version,
    bool force) = 0;

  //----------------------------------------------------------------------------
  //! Obtain keys in the supplied range [start,...,end]
  //!
  //! @param start  the start point of the requested key range, supplied key
  //!   is included in the range
  //! @param end    the end point of the requested key range, supplied key is
  //!   included in the range
  //! @param maxRequested the maximum number of keys requested (cannot be higher
  //!   than limits allow)
  //! @param keys   on success, contains existing key names in supplied range
  //! @return status of operation
  //----------------------------------------------------------------------------
  virtual kinetic::KineticStatus range(
    const std::shared_ptr<const std::string>& start_key,
    const std::shared_ptr<const std::string>& end_key,
    int maxRequested,
    std::unique_ptr< std::vector<std::string> >& keys) = 0;

  //----------------------------------------------------------------------------
  //! Destructor.
  //----------------------------------------------------------------------------
  virtual ~ClusterInterface(){};
};

}



#endif

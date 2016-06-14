//------------------------------------------------------------------------------
//! @file ClusterInterface.hh
//! @author Paul Hermann Lensing
//! @brief Interface to a cluster...
//------------------------------------------------------------------------------

/************************************************************************
 * KineticIo - a file io interface library to kinetic devices.          *
 *                                                                      *
 * This Source Code Form is subject to the terms of the Mozilla         *
 * Public License, v. 2.0. If a copy of the MPL was not                 *
 * distributed with this file, You can obtain one at                    *
 * https://mozilla.org/MP:/2.0/.                                        *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but is provided AS-IS, WITHOUT ANY WARRANTY; including without       *
 * the implied warranty of MERCHANTABILITY, NON-INFRINGEMENT or         *
 * FITNESS FOR A PARTICULAR PURPOSE. See the Mozilla Public             *
 * License for more details.                                            *
 ************************************************************************/

#ifndef KINETICIO_CLUSTERINTERFACE_HH
#define  KINETICIO_CLUSTERINTERFACE_HH

#include <vector>
#include <string>
#include <memory>
#include <functional>
#include "kinetic/kinetic.h"

/* TODO: Do not use kinetic::KineticStatus directly, as it can't
 * represent cluster error states. Would also allow to decouple ClusterInterface
 * from kinetic/kinetic.h
 *
 */

namespace kio {

struct ClusterLimits {
    size_t max_key_size;
    size_t max_version_size;
    size_t max_value_size;
    uint32_t max_range_elements;
};

struct ClusterStats {
    /* Current size values */
    uint64_t bytes_total;
    uint64_t bytes_free;

    /* Current health values */
    //! one or more indicator keys exists
    bool indicator;
    //! percentage of redundancy online
    double robustness;

    /* IO stats total */
    uint64_t read_ops_total;
    uint64_t read_bytes_total;
    uint64_t write_ops_total;
    uint64_t write_bytes_total;

    /* IO stats between io_start and io_end */
    std::chrono::system_clock::time_point io_start;
    std::chrono::system_clock::time_point io_end;
    uint64_t read_ops_period;
    uint64_t read_bytes_period;
    uint64_t write_ops_period;
    uint64_t write_bytes_period;
};

enum class KeyType {
  Data, Metadata
};

class CompareEnum {
public:
  template<typename T>
  bool operator()(const T& lhs, const T& rhs) const
  {
    return static_cast<int>(lhs) < static_cast<int>(rhs);
  }
};

//------------------------------------------------------------------------------
//! Interface to a cluster, primarily intended to interface with Kinetic
//! drives.
//------------------------------------------------------------------------------
class ClusterInterface {
public:
  //----------------------------------------------------------------------------
  //! Obtain identifier of the cluster.
  //!
  //! @return the cluster id
  //----------------------------------------------------------------------------
  virtual const std::string& id() const = 0;

  //----------------------------------------------------------------------------
  //! Obtain unique id for cluster instance (changes for different instances of
  //! the same cluster).
  //!
  //! @return the cluster instance-id
  //----------------------------------------------------------------------------
  virtual const std::string& instanceId() const = 0;

  //----------------------------------------------------------------------------
  //! Obtain maximum key / version / value sizes and maximum number of
  //! elements that can be requested using range().
  //!
  //! These limits may drastically differ from standard Kinetic drive limits.
  //! For example, a value might be split and written to multiple drives. Or
  //! some of the key-space might be reserved for cluster internal metadata.
  //! Limits remain constant during the cluster lifetime.
  //!
  //! @param type limits may differ for data / metadata keys
  //! @return cluster limits
  //----------------------------------------------------------------------------
  virtual const ClusterLimits& limits(KeyType type) const = 0;

  //----------------------------------------------------------------------------
  //! Obtain maximum key / version / value sizes and maximum number of
  //! elements that can be requested using range().
  //!
  //! These limits may drastically differ from standard Kinetic drive limits.
  //! For example, a value might be split and written to multiple drives. Or
  //! some of the key-space might be reserved for cluster internal metadata.
  //! Limits remain constant during the cluster lifetime.
  //!
  //! @return cluster limits
  //----------------------------------------------------------------------------
  virtual ClusterStats stats() = 0;

  //----------------------------------------------------------------------------
  //! Get the value and version associated with the supplied key.
  //
  //! @param key the key
  //! @param version stores the version upon success, not modified on error
  //! @param value stores the value upon success, not modified on error
  //! @param type cluster may handle key types differently (e.g. redundancy)
  //! @return status of operation
  //----------------------------------------------------------------------------
  virtual kinetic::KineticStatus get(
      const std::shared_ptr<const std::string>& key,
      std::shared_ptr<const std::string>& version,
      std::shared_ptr<const std::string>& value,
      KeyType type) = 0;

  //----------------------------------------------------------------------------
  //! Get the version associated with the supplied key. Value will not be
  //! read in from the backend.
  //
  //! @param key the key
  //! @param version stores the version upon success, not modified on error
  //! @param type cluster may handle key types differently (e.g. redundancy)
  //! @return status of operation
  //----------------------------------------------------------------------------
  virtual kinetic::KineticStatus get(
      const std::shared_ptr<const std::string>& key,
      std::shared_ptr<const std::string>& version,
      KeyType type) = 0;

  //----------------------------------------------------------------------------
  //! Write the supplied key-value pair to the cluster. Put is conditional on
  //! the supplied version existing on the cluster.
  //!
  //! @param key the key
  //! @param version existing version expected in the cluster, empty for none.
  //! @param value value to store
  //! @param version_out contains new key version on success
  //! @param type cluster may handle key types differently (e.g. redundancy)
  //! @return status of operation
  //----------------------------------------------------------------------------
  virtual kinetic::KineticStatus put(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version,
      const std::shared_ptr<const std::string>& value,
      std::shared_ptr<const std::string>& version_out,
      KeyType type) = 0;


  //----------------------------------------------------------------------------
  //! Write the supplied key-value pair to the cluster. Put is not conditional,
  //! will always overwrite potentially existing data.
  //!
  //! @param key the key
  //! @param value value to store
  //! @param version_out contains new key version on success
  //! @param type cluster may handle key types differently (e.g. redundancy)
  //! @return status of operation
  //----------------------------------------------------------------------------
  virtual kinetic::KineticStatus put(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& value,
      std::shared_ptr<const std::string>& version_out,
      KeyType type) = 0;

  //----------------------------------------------------------------------------
  //! Delete the key on the cluster, conditional on supplied version matching
  //! the key version existing on the cluster.
  //!
  //! @param key     the key
  //! @param version existing version expected in the cluster
  //! @param type cluster may handle key types differently (e.g. redundancy)
  //! @return status of operation
  //---------------------------------------------------------------------------
  virtual kinetic::KineticStatus remove(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version,
      KeyType type) = 0;

  //----------------------------------------------------------------------------
  //! Force delete the key on the cluster.
  //!
  //! @param key     the key
  //! @param type cluster may handle key types differently (e.g. redundancy)
  //! @return status of operation
  //---------------------------------------------------------------------------
  virtual kinetic::KineticStatus remove(
      const std::shared_ptr<const std::string>& key,
      KeyType type) = 0;

  //----------------------------------------------------------------------------
  //! Flush all connections associated with this cluster. A successful flush
  //! operation guarantees that all previous put and remove operations are
  //! permanently persisted.
  //!
  //! @return status of operation
  //----------------------------------------------------------------------------
  virtual kinetic::KineticStatus flush() = 0;

  //----------------------------------------------------------------------------
  //! Obtain keys in the supplied range [start,...,end]
  //!
  //! @param start  the start point of the requested key range, supplied key
  //!   is included in the range
  //! @param end    the end point of the requested key range, supplied key is
  //!   included in the range
  //! @param keys   on success, contains existing key names in supplied range.
  //! @param type cluster may handle key types differently (e.g. redundancy)
  //! @param max_elements the maximum number of elements to return. 0 signifies
  //!   the max_range_elements of the cluster.
  //! @return status of operation
  //----------------------------------------------------------------------------
  virtual kinetic::KineticStatus range(
      const std::shared_ptr<const std::string>& start_key,
      const std::shared_ptr<const std::string>& end_key,
      std::unique_ptr<std::vector<std::string>>& keys,
      KeyType type, std::size_t max_elements = 0) = 0;

  //----------------------------------------------------------------------------
  //! Destructor.
  //----------------------------------------------------------------------------
  virtual ~ClusterInterface()
  { };
};

}


#endif

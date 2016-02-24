//------------------------------------------------------------------------------
//! @file KineticCluster.hh
//! @author Paul Hermann Lensing
//! @brief General purpose implementation of cluster interface for Kinetic.
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

#ifndef KINETICIO_KINETICCLUSTER_HH
#define  KINETICIO_KINETICCLUSTER_HH

#include "ClusterInterface.hh"
#include "KineticAutoConnection.hh"
#include "KineticCallbacks.hh"
#include "SocketListener.hh"
#include "RedundancyProvider.hh"
#include "StripeOperation.hh"
#include <utility>
#include <chrono>
#include <mutex>

namespace kio {

//------------------------------------------------------------------------------
//! Implementation of cluster interface for arbitrarily sized cluster & stripe
//! sizes.
//------------------------------------------------------------------------------
class KineticCluster : public ClusterInterface {
public:
  //! See documentation in superclass.
  const std::string& id() const;

  //! See documentation in superclass.
  const std::string& instanceId() const;

  //! See documentation in superclass.
  const ClusterLimits& limits(KeyType type) const;

  //! See documentation in superclass.
  ClusterStats stats();

  //! See documentation in superclass.
  kinetic::KineticStatus get(
      const std::shared_ptr<const std::string>& key,
      std::shared_ptr<const std::string>& version,
      std::shared_ptr<const std::string>& value,
      KeyType type);

  //! See documentation in superclass.
  kinetic::KineticStatus get(
      const std::shared_ptr<const std::string>& key,
      std::shared_ptr<const std::string>& version,
      KeyType type);

  //! See documentation in superclass.
  kinetic::KineticStatus put(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version,
      const std::shared_ptr<const std::string>& value,
      std::shared_ptr<const std::string>& version_out,
      KeyType type);

  //! See documentation in superclass.
  kinetic::KineticStatus put(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& value,
      std::shared_ptr<const std::string>& version_out,
      KeyType type);

  //! See documentation in superclass.
  kinetic::KineticStatus remove(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version,
      KeyType type);

  //! See documentation in superclass.
  kinetic::KineticStatus remove(
      const std::shared_ptr<const std::string>& key,
      KeyType type);

  //! See documentation in superclass.
  kinetic::KineticStatus range(
      const std::shared_ptr<const std::string>& start_key,
      const std::shared_ptr<const std::string>& end_key,
      std::unique_ptr<std::vector<std::string>>& keys,
      KeyType type, size_t max_elements = 0);

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param id the cluster name
  //! @param block_size the size of a single data / parity block in bytes
  //! @param operation_timeout the maximum interval an operation is allowed
  //! @param rp_data RedundancyProvider to be used for data keys
  //! @param rp_metadata RedundancyProvider to be used for metadata keys
  //--------------------------------------------------------------------------
  explicit KineticCluster(
      std::string id, std::size_t block_size, std::chrono::seconds operation_timeout,
      std::vector<std::unique_ptr<KineticAutoConnection>> connections,
      std::shared_ptr<RedundancyProvider> rp_data,
      std::shared_ptr<RedundancyProvider> rp_metadata
  );

  //--------------------------------------------------------------------------
  //! Destructor.
  //--------------------------------------------------------------------------
  virtual ~KineticCluster();

protected:
  kinetic::KineticStatus do_remove(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version,
      KeyType type, kinetic::WriteMode mode);

  kinetic::KineticStatus execute_get(kio::StripeOperation_GET& op,
                                       const std::shared_ptr<const std::string>& key,
                                       std::shared_ptr<const std::string>& version,
                                       std::shared_ptr<const std::string>& value, KeyType type);

  kinetic::KineticStatus do_get(
      const std::shared_ptr<const std::string>& key,
      std::shared_ptr<const std::string>& version,
      std::shared_ptr<const std::string>& value, KeyType type, bool skip_value);

  kinetic::KineticStatus do_put(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version,
      const std::shared_ptr<const std::string>& value,
      std::shared_ptr<const std::string>& version_out,
      KeyType type, kinetic::WriteMode mode);

  //--------------------------------------------------------------------------
  //! Update the clusterio statistics and capacity information.
  //--------------------------------------------------------------------------
  void updateStatistics(std::shared_ptr<DestructionMutex> dm);

  //--------------------------------------------------------------------------
  //! Turn a single value into a stripe, complete with redundancy information
  //! 
  //! @param value the value 
  //! @return the stripe build from the value 
  //--------------------------------------------------------------------------
  std::vector<std::shared_ptr<const std::string>> valueToStripe(
      const std::string& value, KeyType type
  );

  //--------------------------------------------------------------------------
  //! Concurrency resolution: In case of partial stripe writes / removes due
  //! to concurrent write accesses, decide which client wins the race based
  //! on achieved write pattern and using remote versions as a tie breaker.
  //!
  //! @param key the key of the stripe
  //! @param version the version the stripe chunks should have, empty
  //!   signifies deleted.
  //! @param rmap the results of the partial put / delete operation causing
  //!   a call to mayForce.
  //! @return true if client may force-overwrite
  //--------------------------------------------------------------------------
  bool mayForce(const std::shared_ptr<const std::string>& key, KeyType type,
                const std::shared_ptr<const std::string>& version, size_t counter = 0);

protected:
  //! cluster id
  const std::string identity;

  // cluster instance id
  const std::string instanceIdentity;

  //! maximum capacity of a single value / parity chunk
  const std::size_t chunkCapacity;

  //! timeout of asynchronous operations
  const std::chrono::seconds operation_timeout;

  //! all connections associated with this cluster
  std::vector<std::unique_ptr<KineticAutoConnection> > connections;

  //! cluster limits are constant over cluster lifetime
  std::map<KeyType, ClusterLimits, CompareEnum> cluster_limits;

  //! erasure coding / replication
  std::map<KeyType, std::shared_ptr<RedundancyProvider>, CompareEnum> redundancy;

  //! time point the clusterio statistics have been last scheduled to be updated
  std::chrono::system_clock::time_point statistics_scheduled;

  //! the cluster statistics
  ClusterStats statistics_snapshot;

  //! prevent background threads accessing member variables after destruction
  std::shared_ptr<DestructionMutex> dmutex;

  //! concurrency control
  std::mutex mutex;
};

}

#endif

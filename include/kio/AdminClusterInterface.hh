//------------------------------------------------------------------------------
//! @file AdminClusterInterface.hh
//! @author Paul Hermann Lensing
//! @brief Interface for cluster status and key scan & repair.
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

#ifndef KINETICIO_ADMINCLUSTERINTERFACE_HH
#define KINETICIO_ADMINCLUSTERINTERFACE_HH

#include <vector>
#include <functional>

namespace kio {

struct ClusterStatus {
  bool indicator_exist;
  uint32_t redundancy_factor;
  uint32_t drives_total;
  uint32_t drives_failed;
  std::vector<bool> connected;
  std::vector<std::string> location;
};

//------------------------------------------------------------------------------
//! Interface class for cluster status and key scan & repair.
//------------------------------------------------------------------------------
class AdminClusterInterface {

public:
  //----------------------------------------------------------------------------
  //! Specify which types of keys should be targeted in an operation. 
  //----------------------------------------------------------------------------
  enum class OperationTarget {
    DATA, METADATA, ATTRIBUTE, INDICATOR, INVALID
  };

  //----------------------------------------------------------------------------
  //! Used to store statistics after a scan or repair operation.
  //----------------------------------------------------------------------------
  struct KeyCounts {
    //! the total number of keys in the cluster
    int total;
    //! #keys where one or more drives the key stripe is stored on are not reachable
    int incomplete;
    //! #keys that are known to require action (disregarding non reachable drives)
    int need_action;
    //! #keys that had one or more subchunks repaired
    int repaired;
    //! #keys that had to be removed (e.g. because a drive was not reachable during a previous remove operation)
    int removed;
    //! #keys were repair / remove was detected to be necessary but failed
    int unrepairable;
  };

  //----------------------------------------------------------------------------
  //! Type of callback function object. If provided it will be called
  //! periodically with the current number of processed keys. If it returns
  //! false the currently executed admin operation will be interrupted.
  //----------------------------------------------------------------------------
  typedef std::function<bool(int)> callback_t;

  //--------------------------------------------------------------------------
  //! Only count the number of keys existing on the cluster.
  //!
  //! @param target the types of keys to be counted
  //! @param callback optionally register a callback function that is called 
  //! with the current number of processed keys periodically
  //! @return the number of keys in the cluster
  //--------------------------------------------------------------------------
  virtual int count(OperationTarget target, callback_t callback = NULL) = 0;

  //--------------------------------------------------------------------------
  //! Scan all subchunks of every target key and check if keys need to
  //! be repaired. This is a scan only, no write operations will occur.
  //!
  //! @param target the types of keys to be scanned 
  //! @param callback optionally register a callback function that is called 
  //! with the current number of processed keys periodically
  //! @param numThreads the number of background IO threads used for scanning
  //! @return statistics about the scanned keys
  //--------------------------------------------------------------------------  
  virtual KeyCounts scan(OperationTarget target, callback_t callback = NULL, int numThreads = 1) = 0;

  //--------------------------------------------------------------------------
  //! Scan all subchunks of every target key and check if keys need to
  //! be repaired. If so, attempt repair.
  //!
  //! @param target the types of keys to be repaired 
  //! @param callback optionally register a callback function that is called 
  //! with the current number of processed keys periodically
  //! @param numThreads the number of background IO threads used for repair
  //! @return statistics about the keys
  //--------------------------------------------------------------------------
  virtual KeyCounts repair(OperationTarget target, callback_t callback = NULL, int numThreads = 1) = 0;

  //--------------------------------------------------------------------------
  //! Force delete keys on the cluster.
  //!
  //! @param target the types of keys to be scanned 
  //! @param callback optionally register a callback function that is called 
  //! with the current number of processed keys periodically
  //! @param numThreads the number of background IO threads used for deletion
  //! @return statistics about the keys
  //--------------------------------------------------------------------------
  virtual KeyCounts reset(OperationTarget target, callback_t callback = NULL, int numThreads = 1) = 0;

  //--------------------------------------------------------------------------
  //! Obtain the current status of connections to all drives attached to this
  //! cluster.
  //!
  //! @return a ClusterStatus structure containing the name and status of each
  //!   connection associated with the cluster, as well as if indicator keys
  //!   have been detected on any healthy connection.
  //--------------------------------------------------------------------------
  virtual ClusterStatus status() = 0;

  virtual ~AdminClusterInterface()
  { };
};

}

#endif //KINETICIO_ADMINCLUSTERINTERFACE_HH

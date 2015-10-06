//------------------------------------------------------------------------------
//! @file AdminClusterInterface.hh
//! @author Paul Hermann Lensing
//! @brief Interface for cluster status and key scan & repair.
//------------------------------------------------------------------------------
#ifndef KINETICIO_ADMINCLUSTERINTERFACE_HH
#define KINETICIO_ADMINCLUSTERINTERFACE_HH

#include <vector>

namespace kio {

//------------------------------------------------------------------------------
//! Interface class for cluster status and key scan & repair.
//------------------------------------------------------------------------------
class AdminClusterInterface {

public:
  //----------------------------------------------------------------------------
  //! Used to store statistics after a scan or repair operation.
  //----------------------------------------------------------------------------
  struct KeyCounts {
    //! the total number of keys in the cluster
    int total;
    //! #keys where one or more drives the key stripe is stored on are not reachable
    int incomplete;
    //! #keys that are known to require repair (disregarding non reachable drives)
    int need_repair;
    //! #keys that had one or more subchunks repaired
    int repaired;
    //! #keys that had to be removed (e.g. because a drive was not reachable during a previous remove operation)
    int removed;
    //! #keys were repair was detected to be necessary but failed
    int unrepairable;
  };

  //--------------------------------------------------------------------------
  //! Only count the number of keys existing on the cluster.
  //!
  //! @param maximum number of keys to count
  //! @param restart if not true, the next call will continue where the
  //!   previous call stopped
  //! @return the number of keys
  //--------------------------------------------------------------------------
  virtual int count(size_t maximum, bool restart=false) = 0;

  //--------------------------------------------------------------------------
  //! Scan all subchunks of every key and check if keys need to
  //! be repaired. This is a scan only, no write operations will occur.
  //!
  //! @param maximum number of keys to scan
  //! @param restart if not true, the next call will continue where the
  //!   previous call stopped
  //! @return statistics of keys
  //--------------------------------------------------------------------------
  virtual KeyCounts scan(size_t maximum, bool restart=false) = 0;

  //--------------------------------------------------------------------------
  //! Scan all subchunks of every key and check if keys need to
  //! be repaired. If so, attempt repair.
  //!
  //! @param maximum number of keys to repair
  //! @param restart if not true, the next call will continue where the
  //!   previous call stopped
  //! @return statistics of keys
  //--------------------------------------------------------------------------
  virtual KeyCounts repair(size_t maximum, bool restart=false) = 0;

  //--------------------------------------------------------------------------
  //! Force delete _all_ keys on the cluster.
  //!
  //! @param maximum number of keys to remove
  //! @param restart if not true, the next call will continue where the
  //!   previous call stopped
  //! @return statistics of keys
  //--------------------------------------------------------------------------
  virtual KeyCounts reset(size_t maximum, bool restart=false) = 0;

  //--------------------------------------------------------------------------
  //! Obtain the current status of connections to all drives attached to this
  //! cluster.
  //!
  //! @return a boolean representation of the connection states
  //--------------------------------------------------------------------------
  virtual std::vector<bool> status() = 0;

  virtual ~AdminClusterInterface(){};
};

}

#endif //KINETICIO_ADMINCLUSTERINTERFACE_HH

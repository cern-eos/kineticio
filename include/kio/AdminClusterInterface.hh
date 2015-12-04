//------------------------------------------------------------------------------
//! @file AdminClusterInterface.hh
//! @author Paul Hermann Lensing
//! @brief Interface for cluster status and key scan & repair.
//------------------------------------------------------------------------------
#ifndef KINETICIO_ADMINCLUSTERINTERFACE_HH
#define KINETICIO_ADMINCLUSTERINTERFACE_HH

#include <vector>
#include <functional>

namespace kio {
  
//------------------------------------------------------------------------------
//! Interface class for cluster status and key scan & repair.
//------------------------------------------------------------------------------
class AdminClusterInterface {

public:
  //----------------------------------------------------------------------------
  //! Specify which types of keys should be targeted in an operation. 
  //----------------------------------------------------------------------------
  enum class OperationTarget{
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

  //--------------------------------------------------------------------------
  //! Only count the number of keys existing on the cluster.
  //!
  //! @param target the types of keys to be counted
  //! @param callback optionally register a callback function that is called 
  //! with the current number of processed keys periodically
  //! @return the number of keys in the cluster
  //--------------------------------------------------------------------------
  virtual int count(OperationTarget target, std::function<void(int)> callback = NULL) = 0;

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
  virtual KeyCounts scan(OperationTarget target, std::function<void(int)> callback = NULL, int numThreads = 1) = 0;

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
  virtual KeyCounts repair(OperationTarget target, std::function<void(int)> callback = NULL, int numThreads = 1) = 0;

  //--------------------------------------------------------------------------
  //! Force delete keys on the cluster.
  //!
  //! @param target the types of keys to be scanned 
  //! @param callback optionally register a callback function that is called 
  //! with the current number of processed keys periodically
  //! @param numThreads the number of background IO threads used for deletion
  //! @return statistics about the keys
  //--------------------------------------------------------------------------
  virtual KeyCounts reset(OperationTarget target, std::function<void(int)> callback = NULL, int numThreads = 1) = 0;

  //--------------------------------------------------------------------------
  //! Obtain the current status of connections to all drives attached to this
  //! cluster.
  //!
  //! @return a boolean representation of the connection states and location
  //!   information for the drives associated with the connection
  //--------------------------------------------------------------------------
  virtual std::vector<std::pair<bool, std::string>> status() = 0;

  virtual ~AdminClusterInterface(){};
};

}

#endif //KINETICIO_ADMINCLUSTERINTERFACE_HH

//------------------------------------------------------------------------------
//! @file KineticAdminCluster.hh
//! @author Paul Hermann Lensing
//! @brief Implemenatation class for cluster status and key scan & repair.
//------------------------------------------------------------------------------
#ifndef KINETICIO_KINETICREPAIRCLUSTER_HH
#define KINETICIO_KINETICREPAIRCLUSTER_HH

#include "KineticCluster.hh"
#include "AdminClusterInterface.hh"

namespace kio{

//------------------------------------------------------------------------------
//! Implemenatation class of interface for cluster status and key scan & repair.
//------------------------------------------------------------------------------
class KineticAdminCluster : public KineticCluster, public AdminClusterInterface {
public:

  //! See documentation of public interface in AdminClusterInterface
  KeyCounts getCounts();
  
  //! See documentation of public interface in AdminClusterInterface
  int count(size_t maximum, bool restart=false);

  //! See documentation of public interface in AdminClusterInterface
  int scan(size_t maximum, bool restart=false);

  //! See documentation of public interface in AdminClusterInterface
  int repair(size_t maximum,bool restart=false);

  //! See documentation of public interface in AdminClusterInterface
  int  reset(size_t maximum, bool restart=false);

  //! See documentation of public interface in AdminClusterInterface
  std::vector<bool> status();

  //! Perfect forwarding is nice, and I am lazy. Look in KineticCluster.hh for the correct arguments
  template<typename... Args>
  KineticAdminCluster(bool indicator_only, size_t numthreads, Args&& ... args) :
      KineticCluster(std::forward<Args>(args)...), 
      indicator_keys(indicator_only), 
      threads(numthreads), 
      bg(new BackgroundOperationHandler(numthreads,0)) 
  {};

  //! Destructor
  ~KineticAdminCluster();

private:
  //--------------------------------------------------------------------------
  //! The different types of admin cluster operations
  //--------------------------------------------------------------------------
  enum class Operation{
      COUNT, SCAN, REPAIR, RESET
  };

  //--------------------------------------------------------------------------
  //! The only difference of this struct to AdminClusterInterface::KeyCounts
  //! is the use of atomics to allow for lockless multithreading.
  //--------------------------------------------------------------------------
  struct KeyCountsInternal{
      std::atomic<int> total;
      std::atomic<int> incomplete;
      std::atomic<int> need_repair;
      std::atomic<int> repaired;
      std::atomic<int> removed;
      std::atomic<int> unrepairable;
      KeyCountsInternal() : total(0), incomplete(0), need_repair(0), repaired(0), removed(0), unrepairable(0) {};
  };
  
  //! keeping track of key counts for continuing operations
  KeyCountsInternal key_counts;  
  
  //! keeping track of the start key for continuing operations
  std::shared_ptr<const std::string> start_key;
  
  //! if set, operations should be performed only on keys marked with indicator keys
  bool indicator_keys;
  
  //! number of background io threads
  size_t threads;
 
  //! handle background operations 
  std::unique_ptr<BackgroundOperationHandler> bg; 
  
  //--------------------------------------------------------------------------
  //! The main loop for count / scan / repair / reset operations.
  //!
  //! @param o The operation type to be executed
  //! @param maximum number of keys to execute the operation on
  //! @param restart continue with last seen key or start fresh
  //! @return statistics of keys
  //--------------------------------------------------------------------------
  int doOperation(Operation o, size_t maximum, bool restart);

  //--------------------------------------------------------------------------
  //! Apply a scan / repair / reset operation to the supplied keys.
  //!
  //! @param keys a set of keys to scan
  //! @param o Operation type
  //! @param counts keep statistics current by increasing repaired / removed
  //!   and unrepairable counts as necessary.
  //--------------------------------------------------------------------------
  void applyOperation(Operation o, std::vector<std::shared_ptr<const std::string>> keys);

  //--------------------------------------------------------------------------
  //! Check if the key requires repair. If the stripe of the key cannot be
  //! read in due to more failures than parities, the function will throw.
  //!
  //! @param key the key of the stripe to test
  //! @param counts  keep statistics current by increasing incomplete and
  //!   need_repair counts as necessary.
  //! @return Returns true if the key needs to be repaired, false if it is
  //! either fine or nothing can be done due to unreachable drives.
  //--------------------------------------------------------------------------
  bool scanKey(const std::shared_ptr<const std::string>& key);
};

}

#endif //KINETICIO_KINETICREPAIRCLUSTER_HH

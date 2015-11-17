//------------------------------------------------------------------------------
//! @file KineticAdminCluster.hh
//! @author Paul Hermann Lensing
//! @brief Implemenatation class for cluster status and key scan & repair.
//------------------------------------------------------------------------------
#ifndef KINETICIO_KINETICADMINCLUSTER_HH
#define KINETICIO_KINETICADMINCLUSTER_HH

#include "KineticCluster.hh"
#include "AdminClusterInterface.hh"

namespace kio{

//------------------------------------------------------------------------------
//! Implemenatation class of interface for cluster status and key scan & repair.
//------------------------------------------------------------------------------
class KineticAdminCluster : public KineticCluster, public AdminClusterInterface {
public:
  //! See documentation of public interface in AdminClusterInterface
  int count(OperationTarget target, std::function<void(int)> callback = NULL);

  //! See documentation of public interface in AdminClusterInterface 
  KeyCounts scan(OperationTarget target, std::function<void(int)> callback = NULL, int numThreads = 1);

  //! See documentation of public interface in AdminClusterInterface
  KeyCounts repair(OperationTarget target, std::function<void(int)> callback = NULL, int numThreads = 1);

  //! See documentation of public interface in AdminClusterInterface
  KeyCounts reset(OperationTarget target, std::function<void(int)> callback = NULL, int numThreads = 1);

  //! See documentation of public interface in AdminClusterInterface
  std::vector<bool> status();

  //! Perfect forwarding is nice, and I am lazy. Look in KineticCluster.hh for the correct arguments
  template<typename... Args>
  KineticAdminCluster(Args&& ... args) : KineticCluster(std::forward<Args>(args)...) {};

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
      std::atomic<int> need_action;
      std::atomic<int> repaired;
      std::atomic<int> removed;
      std::atomic<int> unrepairable;
      KeyCountsInternal() : total(0), incomplete(0), need_action(0), repaired(0), removed(0), unrepairable(0) {};
  };
  
  std::function<void(int)> callback;
  
  //--------------------------------------------------------------------------
  //! The main loop for count / scan / repair / reset operations.
  //!
  //! @param o The operation type to be executed
  //! @param t The operation target
  //! @param numthreads the number of io threads
  //! @return statistics of keys
  //--------------------------------------------------------------------------
  KeyCounts doOperation(Operation o, OperationTarget t, std::function<void(int)> callback, int numthreads);

  //--------------------------------------------------------------------------
  //! Apply a scan / repair / reset operation to the supplied keys.
  //!
  //! @param keys a set of keys to scan
  //! @param o Operation type
  //! @param counts keep statistics current by increasing repaired / removed
  //!   and unrepairable counts as necessary.
  //--------------------------------------------------------------------------
  void applyOperation(Operation o, OperationTarget t, 
    KeyCountsInternal& key_counts, std::vector<std::shared_ptr<const std::string>> keys);

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
  bool scanKey(const std::shared_ptr<const std::string>& key, KeyCountsInternal& key_counts);
  
  //--------------------------------------------------------------------------
  //! Initialize the start_key and end_key variables, depending
  //! on the OperationTarget
  //--------------------------------------------------------------------------  
  void initKeyRange();
};

}

#endif //KINETICIO_KINETICREPAIRCLUSTER_HH

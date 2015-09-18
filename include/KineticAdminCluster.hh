#ifndef KINETICIO_KINETICREPAIRCLUSTER_HH
#define KINETICIO_KINETICREPAIRCLUSTER_HH

#include "KineticCluster.hh"

namespace kio{

class KineticAdminCluster : public KineticCluster {

public:
  struct KeyCounts{
      int total;
      int incomplete;
      int need_repair;
      int repaired;
      int removed;
      int unrepairable;
  };

  // only count keys, don't even scan
  int count();

  //! only scan, do not repair
  KeyCounts scan();

  //! scan & repair entire cluster
  KeyCounts repair();

  //! return status for every connection of the cluster
  std::vector<bool> status();

  //! Perfect forwarding is nice, and I am lazy. Look in KineticCluster.hh for the correct arguments
  template<typename... Args>
  KineticAdminCluster(Args&& ... args) : KineticCluster(std::forward<Args>(args)...) {};

private:
  enum class Operation{
      COUNT, SCAN, REPAIR
  };
  struct KeyCountsInternal{
      std::atomic<int> total;
      std::atomic<int> incomplete;
      std::atomic<int> need_repair;
      std::atomic<int> repaired;
      std::atomic<int> removed;
      std::atomic<int> unrepairable;

      KeyCountsInternal() : total(0), incomplete(0), need_repair(0), repaired(0), removed(0), unrepairable(0) {};
  };

  KeyCounts doOperation(Operation o);

  bool needsRepair(const std::shared_ptr<const std::string>& key, KeyCountsInternal& counts);
  void scanAndRepair(std::vector<std::shared_ptr<const std::string>> keys, Operation o, KeyCountsInternal& counts);
};

}

#endif //KINETICIO_KINETICREPAIRCLUSTER_HH

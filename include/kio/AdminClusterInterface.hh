#ifndef KINETICIO_ADMINCLUSTERINTERFACE_HH
#define KINETICIO_ADMINCLUSTERINTERFACE_HH

#include <vector>

namespace kio {

class KineticAdminClusterInterface {

public:
  struct KeyCounts {
    int total;
    int incomplete;
    int need_repair;
    int repaired;
    int removed;
    int unrepairable;
  };

  // only count keys, don't even scan
  virtual int count() = 0;

  //! only scan, do not repair
  virtual KeyCounts scan() = 0;

  //! scan & repair entire cluster
  virtual KeyCounts repair() = 0;

  //! return status for every connection of the cluster
  virtual std::vector<bool> status() = 0;

  //! destructor
  virtual ~KineticAdminClusterInterface(){};
};

}

#endif //KINETICIO_ADMINCLUSTERINTERFACE_HH

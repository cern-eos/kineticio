#ifndef KINETICASYNCOPERATION_HH
#define	KINETICASYNCOPERATION_HH

#include <kinetic/kinetic.h>
#include <functional>
#include <memory>

class KineticCallback {
public:
  KineticCallback() :
    status(kinetic::KineticStatus(kinetic::StatusCode::CLIENT_INTERNAL_ERROR, "no result")),
    done(false)
    {};
  virtual ~KineticCallback() {}

  void OnResult(kinetic::KineticStatus result) {
    status = result;
    done   = true;
  }

  kinetic::KineticStatus& getResult() {
    return status;
  }

  bool finished(){
    return done;
  }

private:
  kinetic::KineticStatus status;
  bool done;
};


struct KineticAsyncOperation {
  /* The assigned kinetic function. */
  std::function<kinetic::HandlerKey(std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection>&)> function;
  /* The associated callback function. */
  std::shared_ptr<KineticCallback> callback;
  /* The RateLimitConnection assigned by distribute function. */
  RateLimitKineticConnection& connection;
};


#endif	/* KINETICASYNCOPERATION_HH */


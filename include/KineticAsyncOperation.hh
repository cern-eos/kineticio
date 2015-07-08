//------------------------------------------------------------------------------
//! @file KineticAsyncOperation.hh
//! @author Paul Hermann Lensing
//! @brief Structure used to describe an asynchronous kinetic operation
//------------------------------------------------------------------------------
#ifndef KINETICASYNCOPERATION_HH
#define	KINETICASYNCOPERATION_HH

#include <kinetic/kinetic.h>
#include <functional>
#include <memory>

namespace kio{


//------------------------------------------------------------------------------
//! The base class of all callback used by KineticCluster, providing a unified
//! interface to check for completion and return status.
//------------------------------------------------------------------------------
class KineticCallback {
public:
  //----------------------------------------------------------------------------
  //! Marks the callback as finished and sets the supplied result. This function
  //! is called by all subclasses of KineticCallback, but also can be called
  //! directly in case of errors (e.g. connection timeout).
  //!
  //! @param result the result of the operation this callback belongs to.
  //----------------------------------------------------------------------------
  void OnResult(kinetic::KineticStatus result) {
    status = result;
    done   = true;
  }

  //----------------------------------------------------------------------------
  //! Obtain the result of the operation this callback belongs to. Note that
  //! the returned result will only be valid if finished()==true
  //!
  //! @param result the result of the operation this callback belongs to.
  //----------------------------------------------------------------------------
  kinetic::KineticStatus& getResult() {
    return status;
  }

  //----------------------------------------------------------------------------
  //! Check if the callback has been called (and the operation thus completed)
  //!
  //! @return true if the operation has completed, false otherwise.
  //----------------------------------------------------------------------------
  bool finished(){
    return done;
  }

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  KineticCallback() :
    status(kinetic::KineticStatus(kinetic::StatusCode::CLIENT_INTERNAL_ERROR, "no result")),
    done(false) {};

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~KineticCallback() {}

private:
  //! the status / result of the kinetic operation this callback belongs to
  kinetic::KineticStatus status;
  //! true if the associated kinetic operation has completed, false otherwise
  bool done;
};


struct KineticAsyncOperation {
  //! The assigned kinetic function, all arguments except the connection have to be bound.
  std::function<kinetic::HandlerKey(std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection>&)> function;
  //! The associated callback function.
  std::shared_ptr<KineticCallback> callback;
  //! The AutoConnection assigned to this operation, function will be called on the underlying connection. 
  KineticAutoConnection* connection;
};

}

#endif	/* KINETICASYNCOPERATION_HH */


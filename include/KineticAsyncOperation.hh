//------------------------------------------------------------------------------
//! @file KineticAsyncOperation.hh
//! @author Paul Hermann Lensing
//! @brief Structure used to describe an asynchronous kinetic operation
//------------------------------------------------------------------------------
#ifndef KINETICASYNCOPERATION_HH
#define  KINETICASYNCOPERATION_HH

#include <kinetic/kinetic.h>
#include <functional>
#include <memory>
#include <vector>
#include <string>
#include "KineticCallbacks.hh"
#include "KineticAutoConnection.hh"

namespace kio {

struct KineticAsyncOperation {
  //! The assigned kinetic function, all arguments except the connection have to be bound.
  std::function<kinetic::HandlerKey(std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection>&)> function;
  //! The associated callback function.
  std::shared_ptr<KineticCallback> callback;
  //! The AutoConnection assigned to this operation, function will be called on the underlying connection.
  KineticAutoConnection* connection;
};


//! These are helper functions to fill in the function and callback fields in
//! a vector of KineticAsyncOperations
namespace asyncop_fill {

std::shared_ptr<CallbackSynchronization> getVersion(
    std::vector<KineticAsyncOperation>& ops,
    const std::shared_ptr<const std::string>& key
);

std::shared_ptr<CallbackSynchronization> get(
    std::vector<KineticAsyncOperation>& ops,
    const std::shared_ptr<const std::string>& key
);

std::shared_ptr<CallbackSynchronization> put(
    std::vector<KineticAsyncOperation>& ops,
    std::vector<std::shared_ptr<const std::string> >& stripe,
    const std::shared_ptr<const std::string>& key,
    const std::shared_ptr<const std::string>& version_new,
    const std::shared_ptr<const std::string>& version_old,
    kinetic::WriteMode wmode
);

std::shared_ptr<CallbackSynchronization> remove(
    std::vector<KineticAsyncOperation>& ops,
    const std::shared_ptr<const std::string>& key,
    const std::shared_ptr<const std::string>& version,
    kinetic::WriteMode wmode
);

std::shared_ptr<CallbackSynchronization> range(
    std::vector<KineticAsyncOperation>& ops,
    const std::shared_ptr<const std::string>& start_key,
    const std::shared_ptr<const std::string>& end_key,
    int maxRequested
);

std::shared_ptr<CallbackSynchronization> log(
    std::vector<KineticAsyncOperation>& ops,
    const std::vector<kinetic::Command_GetLog_Type> types
);

}
}

#endif	/* KINETICASYNCOPERATION_HH */


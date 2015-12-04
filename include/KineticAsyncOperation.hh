//------------------------------------------------------------------------------
//! @file KineticAsyncOperation.hh
//! @author Paul Hermann Lensing
//! @brief Structure used to describe an asynchronous kinetic operation
//------------------------------------------------------------------------------
#ifndef  KINETICIO_ASYNCOPERATION_HH 
#define  KINETICIO_ASYNCOPERATION_HH

#include <kinetic/kinetic.h>
#include <functional>
#include <memory>
#include <vector>
#include <string>
#include <utility>
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



//! These are helper functions.
//! Fill functions fill in the function and callback fields in a vector of KineticAsyncOperations
//! Check functions check the callbacks for results.
namespace asyncops {

struct VersionCount{
  std::shared_ptr<const std::string> version;
  int frequency;
};

VersionCount mostFrequentRecordVersion(std::vector<KineticAsyncOperation>& ops);
VersionCount mostFrequentVersion(std::vector<KineticAsyncOperation>& ops);



std::unique_ptr<CallbackSynchronization> fillGetVersion(
    std::vector<KineticAsyncOperation>& ops,
    const std::shared_ptr<const std::string>& key
);

std::unique_ptr<CallbackSynchronization> fillGet(
    std::vector<KineticAsyncOperation>& ops,
    const std::shared_ptr<const std::string>& key
);


std::unique_ptr<CallbackSynchronization> fillPut(
    std::vector<KineticAsyncOperation>& ops,
    std::vector<std::shared_ptr<const std::string> >& stripe,
    const std::shared_ptr<const std::string>& key,
    const std::shared_ptr<const std::string>& version_new,
    const std::shared_ptr<const std::string>& version_old,
    kinetic::WriteMode wmode
);

std::unique_ptr<CallbackSynchronization> fillRemove(
    std::vector<KineticAsyncOperation>& ops,
    const std::shared_ptr<const std::string>& key,
    const std::shared_ptr<const std::string>& version,
    kinetic::WriteMode wmode
);

std::unique_ptr<CallbackSynchronization> fillRange(
    std::vector<KineticAsyncOperation>& ops,
    const std::shared_ptr<const std::string>& start_key,
    const std::shared_ptr<const std::string>& end_key,
    size_t maxRequested
);

std::unique_ptr<CallbackSynchronization> fillLog(
    std::vector<KineticAsyncOperation>& ops,
    const std::vector<kinetic::Command_GetLog_Type> types
);

}
}

#endif	/* KINETICASYNCOPERATION_HH */


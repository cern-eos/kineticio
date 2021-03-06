/************************************************************************
 * KineticIo - a file io interface library to kinetic devices.          *
 *                                                                      *
 * This Source Code Form is subject to the terms of the Mozilla         *
 * Public License, v. 2.0. If a copy of the MPL was not                 *
 * distributed with this file, You can obtain one at                    *
 * https://mozilla.org/MP:/2.0/.                                        *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but is provided AS-IS, WITHOUT ANY WARRANTY; including without       *
 * the implied warranty of MERCHANTABILITY, NON-INFRINGEMENT or         *
 * FITNESS FOR A PARTICULAR PURPOSE. See the Mozilla Public             *
 * License for more details.                                            *
 ************************************************************************/

#include "KineticClusterOperation.hh"
#include <Logging.hh>
#include <set>

using namespace kio;
using namespace kinetic;

KineticClusterOperation::KineticClusterOperation(std::vector<std::unique_ptr<KineticAutoConnection>>& connections) :
    sync(std::make_shared<CallbackSynchronization>()), connections(connections)
{ }

KineticClusterOperation::~KineticClusterOperation()
{ }

void KineticClusterOperation::expandOperationVector(std::size_t size, std::size_t offset)
{
  for (size_t i = 0; i < size; i++) {
    operations.push_back(
        KineticAsyncOperation{
            0,
            std::shared_ptr<kio::KineticCallback>(),
            connections[(i + offset) % connections.size()].get()
        }
    );
  }
}

std::map<kinetic::StatusCode, size_t, CompareStatusCode> KineticClusterOperation::executeOperationVector(
    const std::chrono::seconds& timeout)
{
  fd_set a; int fd;
  std::vector<std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection>> cons(operations.size());
  std::vector<kinetic::HandlerKey> hkeys(operations.size());

  /* Call functions on connections. */
  for (size_t i = 0; i < operations.size(); i++) {
    
    /* Skip operations that are already finished. This is most frequently the case in a 2phase get. */
    if(operations[i].callback->finished()) {
      continue;
    }
    
    try {
      cons[i] = operations[i].connection->get();
    }
    catch (const std::system_error& e) {
      operations[i].callback->OnResult(KineticStatus(StatusCode::CLIENT_IO_ERROR, "Connection not available."));
      continue;
    }

    hkeys[i] = operations[i].function(cons[i]);
    if (!cons[i]->Run(&a, &a, &fd)) {
      operations[i].callback->OnResult(KineticStatus(StatusCode::CLIENT_IO_ERROR, "Run returned false."));
      operations[i].connection->setError(cons[i]);
      kio_notice("Failed executing async operation for connection ", operations[i].connection->getName());
    }
  }

  /* Wait until sufficient requests returned or we pass operation timeout. */
  std::chrono::system_clock::time_point timeout_time = std::chrono::system_clock::now() + timeout;
  sync->wait_until(timeout_time);

  /* Timeout any unfinished request. We do not assume connection to be in error state because of a timeout */
  for (size_t i = 0; i < operations.size(); i++) {
    if (!operations[i].callback->finished()) {
      kio_warning("Network timeout (", timeout, ") for connection ", operations[i].connection->getName());
      cons[i]->RemoveHandler(hkeys[i]);
      operations[i].callback->OnResult(KineticStatus(StatusCode::CLIENT_IO_ERROR, "Network timeout"));
    }
  }

  std::map<kinetic::StatusCode, size_t, CompareStatusCode> rmap;
  for (auto it = operations.cbegin(); it != operations.cend(); it++) {
    rmap[it->callback->getResult().statusCode()]++;
  }
  return rmap;
}

ClusterFlushOp::ClusterFlushOp(std::vector<std::unique_ptr<KineticAutoConnection>>& connections)
    : KineticClusterOperation(connections)
{
  expandOperationVector(connections.size(), 0);
  for (auto o = operations.begin(); o != operations.end(); o++) {
    auto cb = std::make_shared<BasicCallback>(sync);
    o->callback = cb;
    o->function = std::bind(&ThreadsafeNonblockingKineticConnection::Flush, std::placeholders::_1, cb);
  }

}

KineticStatus ClusterFlushOp::execute(const std::chrono::seconds& timeout, size_t quorum_size)
{
  auto rmap = executeOperationVector(timeout);

  for (auto it = rmap.cbegin(); it != rmap.cend(); it++) {
    if (it->second >= quorum_size) {
      return KineticStatus(it->first, "");
    }
  }
  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Flush request failed");
}

ClusterLogOp::ClusterLogOp(const std::vector<kinetic::Command_GetLog_Type> types,
                           std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                           std::size_t size, size_t offset) : KineticClusterOperation(connections)
{
  expandOperationVector(size, offset);
  for (auto o = operations.begin(); o != operations.end(); o++) {
    auto cb = std::make_shared<GetLogCallback>(sync);
    o->callback = cb;
    o->function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
        const vector<Command_GetLog_Type>&,
        const shared_ptr<GetLogCallbackInterface>)>(
        &ThreadsafeNonblockingKineticConnection::GetLog,
        std::placeholders::_1,
        types,
        cb);
  }
}

std::vector<std::shared_ptr<GetLogCallback>> ClusterLogOp::execute(
    const std::chrono::seconds& timeout)
{
  executeOperationVector(timeout);

  std::vector<std::shared_ptr<GetLogCallback>> callbacks;
  for (auto o = operations.cbegin(); o != operations.cend(); o++) {
    callbacks.push_back(std::static_pointer_cast<GetLogCallback>(o->callback));
  }
  return callbacks;
}

ClusterRangeOp::ClusterRangeOp(const std::shared_ptr<const std::string>& start_key,
                               const std::shared_ptr<const std::string>& end_key,
                               size_t maxRequestedPerDrive,
                               std::vector<std::unique_ptr<KineticAutoConnection>>& connections)
    : KineticClusterOperation(connections), maxRequested(maxRequestedPerDrive)
{
  bool reverse = *start_key > *end_key; 
  
  expandOperationVector(connections.size(), 0);
  for (auto o = operations.begin(); o != operations.end(); o++) {
    auto cb = std::make_shared<RangeCallback>(sync);
    o->callback = cb;
    o->function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
        const shared_ptr<const string>, bool,
        const shared_ptr<const string>, bool,
        bool,
        int32_t,
        const shared_ptr<GetKeyRangeCallbackInterface>)>(
        &ThreadsafeNonblockingKineticConnection::GetKeyRange,
        std::placeholders::_1,
        reverse ? end_key : start_key, true,
        reverse ? start_key : end_key, true,
        reverse,
        maxRequestedPerDrive,
        cb);
  }
}

kinetic::KineticStatus ClusterRangeOp::execute(const std::chrono::seconds& timeout, size_t quorum_size)
{
  auto rmap = executeOperationVector(timeout);

  for (auto it = rmap.cbegin(); it != rmap.cend(); it++) {
    if (it->second >= quorum_size) {
      return KineticStatus(it->first, "");
    }
  }
  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Range Request failed");
}

void ClusterRangeOp::getKeys(std::unique_ptr<std::vector<std::string>>& keys)
{
  /* merge in set to eliminate doubles */
  std::set<string> set;
  for (auto o = operations.cbegin(); o != operations.cend(); o++) {
    auto& opkeys = std::static_pointer_cast<RangeCallback>(o->callback)->getKeys();
    if (opkeys) {
      set.insert(opkeys->begin(), opkeys->end());
    }
  }
  /* assign to output parameter and cut excess results */
  keys.reset(new std::vector<string>(set.cbegin(), set.cend()));
  if (keys->size() > maxRequested) {
    keys->resize(maxRequested);
  }
}


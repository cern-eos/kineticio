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

#include "ClusterOperation.hh"
#include <Logging.hh>
#include <set>

using namespace kio;
using namespace kinetic;

ClusterOperation::ClusterOperation()
{ }
ClusterOperation::~ClusterOperation()
{ }

void ClusterOperation::expandOperationVector(std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                                             std::size_t size, std::size_t offset)
{
  for(auto i=0; i<size; i++){
    operations.push_back(
        KineticAsyncOperation{
            0,
            std::shared_ptr<kio::KineticCallback>(),
            connections[(i+offset) % connections.size()].get()
        }
    );
  }
}

std::map<kinetic::StatusCode, int, CompareStatusCode> ClusterOperation::executeOperationVector(const std::chrono::seconds& timeout)
{
  kio_debug("Start execution of ", operations.size(), " operations for sync-point ", &sync);
  auto need_retry = false;
  auto rounds_left = 2;
  do {
    rounds_left--;
    std::vector<kinetic::HandlerKey> hkeys(operations.size());

    /* Call functions on connections */
    for (int i = 0; i < operations.size(); i++) {
      try {
        if (operations[i].callback->finished()) {
          continue;
        }
        auto con = operations[i].connection->get();
        hkeys[i] = operations[i].function(con);

        fd_set a;
        int fd;
        if (!con->Run(&a, &a, &fd)) {
          throw std::runtime_error("Connection::Run(...) returned false");
        }
      }
      catch (const std::exception& e) {
        auto status = KineticStatus(StatusCode::CLIENT_IO_ERROR, e.what());
        operations[i].callback->OnResult(status);
        operations[i].connection->setError();
        kio_notice("Failed executing async operation for connection ", operations[i].connection->getName(), status);
      }
    }

    /* Wait until sufficient requests returned or we pass operation timeout. */
    std::chrono::system_clock::time_point timeout_time = std::chrono::system_clock::now() + timeout;
    sync.wait_until(timeout_time);

    need_retry = false;
    for (int i = 0; i < operations.size(); i++) {
      /* timeout any unfinished request*/
      if (!operations[i].callback->finished()) {
        try {
          operations[i].connection->get()->RemoveHandler(hkeys[i]);
        } catch (const std::exception& e) {
          kio_warning("Failed removing handle from connection ", operations[i].connection->getName(), "due to: ",
                      e.what());
        }
        kio_warning("Network timeout for operation ", operations[i].connection->getName(), " for sync-point", &sync);
        auto status = KineticStatus(KineticStatus(StatusCode::CLIENT_IO_ERROR, "Network timeout"));
        operations[i].callback->OnResult(status);
        operations[i].connection->setError();
      }

      /* Retry operations with CLIENT_IO_ERROR code result. Something went wrong with the connection,
       * we might just be able to reconnect and make the problem go away. */
      if (rounds_left && operations[i].callback->getResult().statusCode() == StatusCode::CLIENT_IO_ERROR) {
        operations[i].callback->reset();
        need_retry = true;
      }
    }
  } while (need_retry && rounds_left);

  kio_debug("Finished execution for sync-point ", &sync);

  std::map<kinetic::StatusCode, int, CompareStatusCode> rmap;
  for (auto it = operations.begin(); it != operations.end(); it++) {
    rmap[it->callback->getResult().statusCode()]++;
  }
  return rmap;
}

ClusterLogOp::ClusterLogOp(const std::vector<kinetic::Command_GetLog_Type> types,
                           std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                           std::size_t size, size_t offset) : ClusterOperation()
{
  expandOperationVector(connections, size, offset);
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
  for (auto o = operations.begin(); o != operations.end(); o++) {
    callbacks.push_back(std::static_pointer_cast<GetLogCallback>(o->callback));
  }
  return callbacks;
}

ClusterRangeOp::ClusterRangeOp(const std::shared_ptr<const std::string>& start_key,
                               const std::shared_ptr<const std::string>& end_key,
                               size_t maxRequestedPerDrive,
                               std::vector<std::unique_ptr<KineticAutoConnection>>& connections)
    : maxRequested(maxRequestedPerDrive)
{
  expandOperationVector(connections, connections.size(), 0);
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
        std::cref(start_key), true,
        std::cref(end_key), true,
        false,
        maxRequestedPerDrive,
        cb);
  }
}

kinetic::KineticStatus ClusterRangeOp::execute(const std::chrono::seconds& timeout,
                                               std::shared_ptr<RedundancyProvider>& redundancy)
{
  auto rmap = executeOperationVector(timeout);

  for (auto it = rmap.cbegin(); it != rmap.cend(); it++) {
    if (it->second > operations.size() - redundancy->size()) {
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
  keys.reset(new std::vector<string>(set.begin(), set.end()));
  if (keys->size() > maxRequested) {
    keys->resize(maxRequested);
  }
}


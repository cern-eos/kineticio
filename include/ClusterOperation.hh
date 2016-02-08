//------------------------------------------------------------------------------
//! @file ClusterOperation.hh
//! @author Paul Hermann Lensing
//! @brief Operations on connections of a cluster
//------------------------------------------------------------------------------

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

#ifndef  KINETICIO_CLUSTEROPERATION_HH
#define  KINETICIO_CLUSTEROPERATION_HH

#include <kinetic/kinetic.h>
#include <functional>
#include <memory>
#include <vector>
#include <string>
#include <utility>
#include "KineticCallbacks.hh"
#include "KineticAutoConnection.hh"
#include "RedundancyProvider.hh"


namespace kio {

//--------------------------------------------------------------------------
//! Compare kinetic::StatusCode, always evaluating regular results smaller
//! than error codes so iterating through result map hits regular results
//! first.
//--------------------------------------------------------------------------
class CompareStatusCode {
private:
  int toInt(const kinetic::StatusCode& code) const
  {
    if (code != kinetic::StatusCode::OK &&
        code != kinetic::StatusCode::REMOTE_NOT_FOUND &&
        code != kinetic::StatusCode::REMOTE_VERSION_MISMATCH) {
      return static_cast<int>(code) + 100;
    }
    return static_cast<int>(code);
  }

public:
  bool operator()(const kinetic::StatusCode& lhs, const kinetic::StatusCode& rhs) const
  {
    return toInt(lhs) < toInt(rhs);
  }
};

//--------------------------------------------------------------------------
//! Cluster Operation base class, not intended to be used directly
//--------------------------------------------------------------------------
class ClusterOperation {
public:
  //--------------------------------------------------------------------------
  //! Constructor.
  //--------------------------------------------------------------------------
  explicit ClusterOperation();

  //--------------------------------------------------------------------------
  //! Destructor.
  //--------------------------------------------------------------------------
  virtual ~ClusterOperation();

  //--------------------------------------------------------------------------
  //! Executes an operation vector. The operation vector will have to have been
  //! set up by one of the child classes
  //!
  //! @param timeout the network timeout to be used
  //! @return a std::map containing the frequency of operation results
  //--------------------------------------------------------------------------
  std::map<kinetic::StatusCode, int, CompareStatusCode> executeOperationVector(const std::chrono::seconds& timeout);

protected:
  struct KineticAsyncOperation {
      //! The assigned kinetic function, all arguments except the connection have to be bound.
      std::function<kinetic::HandlerKey(std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection>&)> function;
      //! The associated callback function.
      std::shared_ptr<KineticCallback> callback;
      //! The AutoConnection assigned to this operation, function will be called on the underlying connection.
      KineticAutoConnection* connection;
  };

  //! Operation vector
  std::vector<KineticAsyncOperation> operations;

  //! Callback syncronization
  CallbackSynchronization sync;

  //--------------------------------------------------------------------------
  //! Used for initial setup (and possible future expansion) of the operation
  //! vector. Chooses the connections to be used. Can be overwritten for
  //! different selection strategies.
  //!
  //! @param connections the connections to be used
  //! @param size the number of operations to execute
  //! @param offset the offset to add to the initial connection choice
  //--------------------------------------------------------------------------
  virtual void expandOperationVector(
      std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
      std::size_t size, std::size_t offset
  );
};


//--------------------------------------------------------------------------
//! A range operation
//--------------------------------------------------------------------------
class ClusterRangeOp : public ClusterOperation {
  /* Allow StripeOperation_GET access to internals in order to identify
   * connections for handoff keys */
  friend class StripeOperation_GET;
public:
  //--------------------------------------------------------------------------
  //! Return the keys
  //!
  //! @param keys structure to fill in
  //--------------------------------------------------------------------------
  void getKeys(std::unique_ptr<std::vector<std::string>>& keys);

  //--------------------------------------------------------------------------
  //! Executes the operation set up in the constructor and returns the overall
  //! status. If status evaluates to ok(), getKeys() may be used to obtain the
  //! keys returned by the range request.
  //!
  //! @param timeout the network timeout
  //! @param redundancy the redundancy object
  //! @return status of the execution
  //--------------------------------------------------------------------------
  kinetic::KineticStatus execute(const std::chrono::seconds& timeout, std::shared_ptr<RedundancyProvider>& redundancy);

  //--------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param start_key start key
  //! @param end_key end key
  //! @param maxRequestedPerDrive maximum number of keys to be returned
  //! @param connections connection vector of the calling cluster
  //--------------------------------------------------------------------------
  ClusterRangeOp(
      const std::shared_ptr<const std::string>& start_key,
      const std::shared_ptr<const std::string>& end_key,
      size_t maxRequestedPerDrive,
      std::vector<std::unique_ptr<KineticAutoConnection>>& connections
  );

private:
  //! the number of maximum requested keys
  size_t maxRequested;
};


//--------------------------------------------------------------------------
//! A log operation
//--------------------------------------------------------------------------
class ClusterLogOp : public ClusterOperation {
public:
  //--------------------------------------------------------------------------
  //! Execute the operation and return individual callbacks in a vector.
  //! The getlog execute operation differently from other ClusterOperations
  //! does not return an overall state, since it is not well defined.
  //!
  //! @param timeout the network timeout
  //! @return vector of callbacks for the executed operations
  //--------------------------------------------------------------------------
  std::vector<std::shared_ptr<GetLogCallback>> execute(const std::chrono::seconds& timeout);

  //--------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param types the log types to be requested
  //! @param connections the connection vector of the calling cluster
  //! @param size number of drives to request logs from
  //! @param offset the offset for choosing the first drive
  //--------------------------------------------------------------------------
  ClusterLogOp(
      const std::vector<kinetic::Command_GetLog_Type> types,
      std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
      std::size_t size, std::size_t offset = 0
  );
};

}

#endif


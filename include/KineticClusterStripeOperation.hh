//------------------------------------------------------------------------------
//! @file KineticClusterStripeOperation.hh
//! @author Paul Hermann Lensing
//! @brief put / get / delete operations on striped data (ec or replication)
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

#ifndef  KINETICIO_KINETICCLUSTERSTRIPEOPERATION_HH
#define  KINETICIO_KIENTICCLUSTERSTRIPEOPERATION_HH

#include <kinetic/kinetic.h>
#include <functional>
#include <memory>
#include <vector>
#include <string>
#include <utility>
#include "KineticCallbacks.hh"
#include "KineticAutoConnection.hh"
#include "RedundancyProvider.hh"
#include "KineticClusterOperation.hh"

namespace kio {

//--------------------------------------------------------------------------
//! Stripe Operation base class, expand on ClusterOperation by providing
//! indicator key support and basing operation vector connection choices
//! on supplied key.
//--------------------------------------------------------------------------
class KineticClusterStripeOperation : public KineticClusterOperation {
public:
  //--------------------------------------------------------------------------
  //! After executing a stripe operation, this function can be used to check
  //! if an indicator key should be placed for the stripe.
  //!
  //! @return true if indicator should be placed, false otherwise
  //--------------------------------------------------------------------------
  bool needsIndicator();

  //--------------------------------------------------------------------------
  //! Place an indicator key
  //!
  //! @param connection vector of the cluster the indicator is supposed
  //!   to be placed on
  //--------------------------------------------------------------------------
  void putIndicatorKey();

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param key the key associated with this stripe operation
  //--------------------------------------------------------------------------
  explicit KineticClusterStripeOperation(
      std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
      const std::shared_ptr<const std::string>& key,
      std::shared_ptr<RedundancyProvider>& redundancy
  );

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  virtual ~KineticClusterStripeOperation();

protected:
  //--------------------------------------------------------------------------
  //! Overwrite base ClusterOperation operation vector generation, taking
  //! into account the key supplied to the constructor for placement.
  //!
  //! @param connections the connections to be used
  //! @param size the number of operations to execute
  //! @param offset the offset to add to the initial connection choice
  //--------------------------------------------------------------------------
  void expandOperationVector(
      std::size_t size,
      std::size_t offset
  );

  //--------------------------------------------------------------------------
  //! Create a single key containing the supplied name/version/value on any
  //! connection.
  //! @param name the key name to put
  //! @param version the version to put ss
  //! @param value the value to put
  //! @param connections the connection vector
  //! @return status of the put operation
  //--------------------------------------------------------------------------
  kinetic::KineticStatus createSingleKey(
      std::shared_ptr<const std::string> name,
      std::shared_ptr<const std::string> version,
      std::shared_ptr<const std::string> value
  );

  //! the key associated wit this stripe operation
  const std::shared_ptr<const std::string>& key;
  //! initialized as false, set to true when a situation requiring and indicator key is detected
  bool need_indicator;
  //! redundancy provider for the stripe operation
  std::shared_ptr<RedundancyProvider>& redundancy;
};


//--------------------------------------------------------------------------
//! Stripe Get Operation
//--------------------------------------------------------------------------
class StripeOperation_GET : public KineticClusterStripeOperation {
public:
  //--------------------------------------------------------------------------
  //! Constructor, sets up the operation vector.
  //!
  //! @params... all the params
  //--------------------------------------------------------------------------
  explicit StripeOperation_GET(const std::shared_ptr<const std::string>& key, bool skip_value,
                               std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                               std::shared_ptr<RedundancyProvider>& redundancy, bool skip_partial_get = false);

  //--------------------------------------------------------------------------
  //! Execute the operation vector set up in the constructor and evaluate
  //! results. Will throw if version / value can not be read in
  //!
  //! @param timeout the network timeout
  //! @return returns operation status
  //--------------------------------------------------------------------------
  kinetic::KineticStatus execute(const std::chrono::seconds& timeout);

  //--------------------------------------------------------------------------
  //! Return the value if execute succeeded
  //!
  //! @return the value
  //--------------------------------------------------------------------------
  std::shared_ptr<const std::string> getValue() const;

  //--------------------------------------------------------------------------
  //! Return the version if execute succeeded
  //!
  //! @return the version
  //--------------------------------------------------------------------------
  std::shared_ptr<const std::string> getVersion() const;

  //--------------------------------------------------------------------------
  //! Returns the version of the sub-chunk indicated by the index.
  //!
  //! @return the version of the indicated subchunk
  //--------------------------------------------------------------------------
  std::shared_ptr<const std::string> getVersionAt(int index) const;

  //--------------------------------------------------------------------------
  //! Structure to store a version and it's frequency in the operation vector
  //--------------------------------------------------------------------------
  struct VersionCount {
    std::shared_ptr<const std::string> version;
    size_t frequency;
  };

  //--------------------------------------------------------------------------
  //! Return the most frequent version and its frequency. Does NOT count
  //! non-existing values as empty version.
  //!
  //! @return the most frequent version and its frequency
  //--------------------------------------------------------------------------
  VersionCount mostFrequentVersion() const;

protected:
  //--------------------------------------------------------------------------
  //! Fill in functions and callbacks for operations that only have their
  //! connection set.
  //--------------------------------------------------------------------------
  void fillOperationVector();

  //--------------------------------------------------------------------------
  //! Fill in function and callback for the supplied operation
  //--------------------------------------------------------------------------
  void fillOperation(KineticAsyncOperation& op, std::shared_ptr<const std::string> key);

  //--------------------------------------------------------------------------
  //! Reconstruct the value from the operation vector if possible
  //!
  //! @return redundancy the redundancy object to recompute missing chunks
  //! @return chunkCapacity the chunk capacity
  //--------------------------------------------------------------------------
  void reconstructValue();

  //--------------------------------------------------------------------------
  //! Searches all supplied connections for existing handoff keys. If any are
  //! found for the currently most frequent version, the stripe operation's
  //! operation vector will be modified so that the next call to execution()
  //! will access the handoff keys instead of the 'normal' keys for a chunk.
  //!
  //! @param connections the connection vector of the calling cluster
  //! @return true if any operations were modified, false otherwise
  //--------------------------------------------------------------------------
  bool insertHandoffChunks();

  kinetic::KineticStatus do_execute(const std::chrono::seconds& timeout);

  //! metadata only get
  bool skip_value;
  //! the most frequent version in the operation vector
  VersionCount version;
  //! the reconstructed value
  std::shared_ptr<std::string> value;
};


//----------------------------------------------------------------------------
//! Abstract base class for modifying stripe operations (put, del). Implements
//! resolving of partial writes (concurrency / race conditions).
//----------------------------------------------------------------------------
class WriteStripeOperation : public KineticClusterStripeOperation {
protected:
  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @params... all the params
  //--------------------------------------------------------------------------
  explicit WriteStripeOperation(
      std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
      const std::shared_ptr<const std::string>& key,
      std::shared_ptr<RedundancyProvider>& redundancy
  );

  //--------------------------------------------------------------------------
  //! Pure virtual method specifying interface to fill in the asynchronous
  //! operation at the given index.
  //!
  //! @param index index into the operation vector
  //! @param drive_version the version expected to be encountered on the drive
  //! @param writeMode versioned or force put
  //--------------------------------------------------------------------------
  virtual void fillOperation(
      size_t index,
      const std::shared_ptr<const std::string>& drive_version,
      kinetic::WriteMode writeMode
  ) = 0;

  //--------------------------------------------------------------------------
  //! Concurrency resolution: In case of partial stripe writes / removes due
  //! to concurrent write accesses, decide which client wins the race based
  //! on achieved write pattern and using remote versions as a tie breaker.
  //!
  //! @param timeout the timeout value to use for any put / get operations
  //! @param version the target version of the stripe, a size zero string
  //!   indicates that the stripe should be removed.
  //--------------------------------------------------------------------------
  void resolvePartialWrite(
      const std::chrono::seconds& timeout,
      const std::shared_ptr<const std::string>& version
  );

private:
  //! write access has been granted, overwrite the stripe chunks that have the wrong version
  bool attemptStripeRepair(const std::chrono::seconds& timeout, const StripeOperation_GET& drive_versions);
};

//--------------------------------------------------------------------------
//! Stripe Put Operation
//--------------------------------------------------------------------------
class StripeOperation_PUT : public WriteStripeOperation {
public:
  //--------------------------------------------------------------------------
  //! Write handoff keys
  //!
  //! @param connection vector of the cluster the handoff keys are supposed
  //!   to be placed on
  //--------------------------------------------------------------------------
  void putHandoffKeys();

  //--------------------------------------------------------------------------
  //! Execute the operation vector set up in the constructor and evaluate
  //! results. Will throw if a partial stripe write is detected.
  //!
  //! @param timeout the network timeout
  //! @param redundancy nData & nParity information
  //--------------------------------------------------------------------------
  kinetic::KineticStatus execute(const std::chrono::seconds& timeout);

  //--------------------------------------------------------------------------
  //! Constructor, sets up the operation vector.
  //!
  //! @params... all the params
  //--------------------------------------------------------------------------
  explicit StripeOperation_PUT(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version_new,
      const std::shared_ptr<const std::string>& version_old,
      std::vector<std::shared_ptr<const std::string>>& values,
      kinetic::WriteMode writeMode,
      std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
      std::shared_ptr<RedundancyProvider>& redundancy
  );

private:
  //! see parent class
  void fillOperation(
      size_t index,
      const std::shared_ptr<const std::string>& drive_version,
      kinetic::WriteMode writeMode
  );

private:
  //! remember target version in case we need to write handoff keys
  const std::shared_ptr<const std::string>& version_new;
  //! remember chunk values in case we write handoff keys or repair a stripe
  std::vector<std::shared_ptr<const std::string>>& values;
};

//--------------------------------------------------------------------------
//! Stripe Remove Operation
//--------------------------------------------------------------------------
class StripeOperation_DEL : public WriteStripeOperation {
public:
  //--------------------------------------------------------------------------
  //! Execute the operation vector set up in the constructor and evaluate
  //! results. Will throw if a partial stripe write is detected.
  //!
  //! @param timeout the network timeout
  //! @param redundancy nData & nParity information
  //--------------------------------------------------------------------------
  kinetic::KineticStatus execute(const std::chrono::seconds& timeout);

  //--------------------------------------------------------------------------
  //! Constructor, sets up the operation vector.
  //!
  //! @params... all the params
  //--------------------------------------------------------------------------
  explicit StripeOperation_DEL(
      const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version,
      kinetic::WriteMode writeMode,
      std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
      std::shared_ptr<RedundancyProvider>& redundancy,
      std::size_t size, std::size_t offset = 0
  );

private:
  //! see parent class
  void fillOperation(
      size_t index,
      const std::shared_ptr<const std::string>& drive_version,
      kinetic::WriteMode writeMode
  );
};

}

#endif


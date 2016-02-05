//------------------------------------------------------------------------------
//! @file StripeOperation.hh
//! @author Paul Hermann Lensing
//! @brief StripeOperation
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

#ifndef  KINETICIO_STRIPEOPERATION_HH
#define  KINETICIO_STRIPEOPERATION_HH

#include <kinetic/kinetic.h>
#include <functional>
#include <memory>
#include <vector>
#include <string>
#include <utility>
#include "KineticCallbacks.hh"
#include "KineticAutoConnection.hh"
#include "RedundancyProvider.hh"
#include "ClusterOperation.hh"

namespace kio {

//--------------------------------------------------------------------------
//! Stripe Operation base class, expand on ClusterOperation by providing
//! indicator key support and basing operation vector connection choices
//! on supplied key.
//--------------------------------------------------------------------------
class StripeOperation : public ClusterOperation {
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
  void putIndicatorKey(std::vector<std::unique_ptr<KineticAutoConnection>>& connections);

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param key the key associated with this stripe operation
  //--------------------------------------------------------------------------
  explicit StripeOperation(const std::shared_ptr<const std::string>& key);

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  virtual ~StripeOperation();

protected:
  //--------------------------------------------------------------------------
  //! Overwrite base ClusterOperation operation vector generation, taking
  //! into account the key supplied to the constructor for placement.
  //!
  //! @param connections the connections to be used
  //! @param size the number of operations to execute
  //! @param offset the offset to add to the initial connection choice
  //--------------------------------------------------------------------------
  void expandOperationVector(std::vector<std::unique_ptr<KineticAutoConnection>>& connections, std::size_t size,std::size_t offset);

  //! the key associated wit this stripe operation
  const std::shared_ptr<const std::string>& key;
  //! initialized as false, set to true when a situation requiring and indicator key is detected
  bool need_indicator;
};

//--------------------------------------------------------------------------
//! Stripe Put Operation
//--------------------------------------------------------------------------
class StripeOperation_PUT : public StripeOperation {
public:
  //--------------------------------------------------------------------------
  //! Execute the operation vector set up in the constructor and evaluate
  //! results. Will throw if a partial stripe write is detected.
  //!
  //! @param timeout the network timeout
  //! @param redundancy nData & nParity information
  //--------------------------------------------------------------------------
  kinetic::KineticStatus execute(const std::chrono::seconds& timeout, std::shared_ptr<RedundancyProvider>& redundancy);

  //--------------------------------------------------------------------------
  //! Constructor, sets up the operation vector.
  //!
  //! @params... all the params
  //--------------------------------------------------------------------------
  explicit StripeOperation_PUT(const std::shared_ptr<const std::string>& key,
                      const std::shared_ptr<const std::string>& version_new,
                      const std::shared_ptr<const std::string>& version_old,
                      std::vector<std::shared_ptr<const std::string>>& values,
                      kinetic::WriteMode writeMode,
                      std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                      std::size_t size, std::size_t offset = 0);
};

//--------------------------------------------------------------------------
//! Stripe Remove Operation
//--------------------------------------------------------------------------
class StripeOperation_DEL : public StripeOperation {
public:
  //--------------------------------------------------------------------------
  //! Execute the operation vector set up in the constructor and evaluate
  //! results. Will throw if a partial stripe write is detected.
  //!
  //! @param timeout the network timeout
  //! @param redundancy nData & nParity information
  //--------------------------------------------------------------------------
  kinetic::KineticStatus execute(const std::chrono::seconds& timeout, std::shared_ptr<RedundancyProvider>& redundancy);

  //--------------------------------------------------------------------------
  //! Constructor, sets up the operation vector.
  //!
  //! @params... all the params
  //--------------------------------------------------------------------------
  explicit StripeOperation_DEL(const std::shared_ptr<const std::string>& key,
                      const std::shared_ptr<const std::string>& version,
                      kinetic::WriteMode writeMode,
                      std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                      std::size_t size, std::size_t offset = 0);
};

//--------------------------------------------------------------------------
//! Stripe Get Operation
//--------------------------------------------------------------------------
class StripeOperation_GET : public StripeOperation {
public:
  //--------------------------------------------------------------------------
  //! Constructor, sets up the operation vector.
  //!
  //! @params... all the params
  //--------------------------------------------------------------------------
  explicit StripeOperation_GET(const std::shared_ptr<const std::string>& key, bool skip_value,
                      std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                      std::size_t size, std::size_t offset = 0);

  //--------------------------------------------------------------------------
  //! Extend the operation vector set up by the constructor by size elements.
  //! This function can be used to add parity chunks to the opreation vector.
  //! Already successfully executed operations will not be re-tried.
  //!
  //! @param connections the connection vector of the calling cluster
  //! @size the number of operations to add to the operation vector
  //--------------------------------------------------------------------------
  void extend(std::vector<std::unique_ptr<KineticAutoConnection>>& connections, std::size_t size);

  //--------------------------------------------------------------------------
  //! Execute the operation vector set up in the constructor and evaluate
  //! results. Will throw if version / value can not be read in
  //!
  //! @param timeout the network timeout
  //! @param redundancy nData & nParity information
  //! @param chunkCapacity Only used in case of skip_value=false to verify
  //!   chunk size. TODO: recode to not require chunkcapacity
  //--------------------------------------------------------------------------
  kinetic::KineticStatus execute(const std::chrono::seconds& timeout,
                                 std::shared_ptr<RedundancyProvider>& redundancy,
                                 std::size_t chunkCapacity);

  //--------------------------------------------------------------------------
  //! Return the value if execute succeeded
  //!
  //! @return the value
  //--------------------------------------------------------------------------
  std::shared_ptr<const std::string> getValue();

  //--------------------------------------------------------------------------
  //! Return the version if execute succeeded
  //!
  //! @return the version
  //--------------------------------------------------------------------------
  std::shared_ptr<const std::string> getVersion();

  //--------------------------------------------------------------------------
  //! Structure to store a version and it's frequency in the operation vector
  //--------------------------------------------------------------------------
  struct VersionCount {
      std::shared_ptr<const std::string> version;
      int frequency;
  };

  //--------------------------------------------------------------------------
  //! Return the most frequent version and its frequency. Does not count
  //! non-existing values as empty version as kinetic sometimes does.
  //!
  //! @return the most frequent version and its frequency
  //--------------------------------------------------------------------------
  VersionCount mostFrequentVersion();

  //--------------------------------------------------------------------------
  //! Check which position the supplied version has in the operation vector
  //! (if any).
  //!
  //! @param version version to check
  //! @return first index position in operation vector, operation size if not
  //!   available
  //--------------------------------------------------------------------------
  size_t versionPosition(const std::shared_ptr<const std::string>& version) const;

private:
  //--------------------------------------------------------------------------
  //! Fill in functions and callbacks for operations that only have their
  //! connection set.
  //--------------------------------------------------------------------------
  void fillOperationVector();

  //--------------------------------------------------------------------------
  //! Reconstruct the value from the operation vector if possible
  //!
  //! @return redundancy the redundancy object to recompute missing chunks
  //! @return chunkCapacity the chunk capacity
  //--------------------------------------------------------------------------
  void reconstructValue(std::shared_ptr<RedundancyProvider>& redundancy, std::size_t chunkCapacity);

  //! metadata only get
  bool skip_value;
  //! the most frequent version in the opreation vector
  VersionCount version;
  //! the reconstructed value
  std::shared_ptr<std::string> value;
};

}

#endif


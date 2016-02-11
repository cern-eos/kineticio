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

#include <Logging.hh>
#include <RedundancyProvider.hh>
#include <ClusterInterface.hh>
#include "StripeOperation.hh"
#include "outside/MurmurHash3.h"
#include <set>

using namespace kio;
using namespace kinetic;


StripeOperation::StripeOperation(const std::shared_ptr<const std::string>& key) : key(key), need_indicator(false)
{
}

StripeOperation::~StripeOperation()
{

}

void StripeOperation::expandOperationVector(std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                                            std::size_t size,
                                            std::size_t offset)
{
  std::uint32_t index;
  MurmurHash3_x86_32(key->c_str(), key->length(), 0, &index);
  index += offset;

  while (size) {
    index = (index + 1) % connections.size();
    operations.push_back(
        KineticAsyncOperation{
            0,
            std::shared_ptr<kio::KineticCallback>(),
            connections[index].get()
        }
    );
    size--;
  }
}

namespace {

std::shared_ptr<KineticRecord> makeRecord(const std::shared_ptr<const std::string>& value,
                                          const std::shared_ptr<const std::string>& version)
{
  /* Generate Checksum */
  auto checksum = crc32c(0, value->c_str(), value->length());
  auto tag = std::make_shared<string>(
      std::to_string((long long unsigned int) checksum)
  );

  /* Construct record structure. */
  return std::make_shared<KineticRecord>(
      value, version, tag, com::seagate::kinetic::client::proto::Command_Algorithm_CRC32
  );
}

bool validStatusCode(const kinetic::StatusCode& code)
{
  return code == StatusCode::OK || code == StatusCode::REMOTE_NOT_FOUND || code == StatusCode::REMOTE_VERSION_MISMATCH;
}

}

// todo: try_put
kinetic::KineticStatus StripeOperation::createSingleKey(std::shared_ptr<const std::string> keyname,
                                                        std::shared_ptr<const std::string> keyversion,
                                                        std::shared_ptr<const std::string> keyvalue,
                                                        std::vector<std::unique_ptr<KineticAutoConnection>>& connections)
{
  auto record = makeRecord(keyvalue, keyversion);
  auto count = 0;

  do {
    expandOperationVector(connections, 1, operations.size());
    count++;

    auto cb = std::make_shared<PutCallback>(sync);
    operations.back().callback = cb;
    operations.back().function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
        const shared_ptr<const string>,
        const shared_ptr<const string>,
        WriteMode,
        const shared_ptr<const KineticRecord>,
        const shared_ptr<PutCallbackInterface>,
        PersistMode)>(
        &ThreadsafeNonblockingKineticConnection::Put,
        std::placeholders::_1,
        keyname,
        make_shared<const string>(),
        WriteMode::REQUIRE_SAME_VERSION,
        record,
        cb,
        PersistMode::WRITE_BACK);

    executeOperationVector(std::chrono::seconds(5));
  } while (!operations.back().callback->getResult().ok() &&
           operations.back().callback->getResult().statusCode() != StatusCode::REMOTE_VERSION_MISMATCH &&
           count < connections.size());

  kio_notice("Single key put ", *keyname, " with result ", operations.back().callback->getResult());
  return operations.back().callback->getResult();
}

void StripeOperation::putIndicatorKey(std::vector<std::unique_ptr<KineticAutoConnection>>& connections)
{
  createSingleKey(utility::makeIndicatorKey(*key), make_shared<const string>("indicator"),
                  make_shared<const string>(), connections);
}

bool StripeOperation::needsIndicator()
{
  return need_indicator;
}


StripeOperation_PUT::StripeOperation_PUT(const std::shared_ptr<const std::string>& key,
                                         const std::shared_ptr<const std::string>& version_new,
                                         const std::shared_ptr<const std::string>& version_old,
                                         std::vector<std::shared_ptr<const std::string>>& values,
                                         kinetic::WriteMode writeMode,
                                         std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                                         std::size_t size, size_t offset)
    : StripeOperation(key), version_new(version_new), values(values)
{
  expandOperationVector(connections, size, offset);
  for (int i = 0; i < operations.size(); i++) {

    auto record = makeRecord(values[i], version_new);
    auto cb = std::make_shared<PutCallback>(sync);

    operations[i].callback = cb;
    operations[i].function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
        const shared_ptr<const string>,
        const shared_ptr<const string>,
        WriteMode,
        const shared_ptr<const KineticRecord>,
        const shared_ptr<PutCallbackInterface>,
        PersistMode)>(
        &ThreadsafeNonblockingKineticConnection::Put,
        std::placeholders::_1,
        key,
        version_old,
        writeMode,
        record,
        cb,
        PersistMode::WRITE_BACK);
  }
}

kinetic::KineticStatus StripeOperation_PUT::execute(const std::chrono::seconds& timeout,
                                                    std::shared_ptr<RedundancyProvider>& redundancy)
{
  auto rmap = executeOperationVector(timeout);

  /* Partial stripe write has to be resolved. */
  if (rmap[StatusCode::OK] && (rmap[StatusCode::REMOTE_VERSION_MISMATCH] || rmap[StatusCode::REMOTE_NOT_FOUND])) {
    throw std::runtime_error("Partial Stripe Write Detected.");
  }

  for (auto it = rmap.cbegin(); it != rmap.cend(); it++) {
    if (it->second >= redundancy->numData()) {
      if (it->first == StatusCode::OK && it->second < redundancy->size()) {
        need_indicator = true;
      }
      return KineticStatus(it->first, "");
    }
  }
  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Key" + *key + "not accessible.");
}


void StripeOperation_PUT::putHandoffKeys(std::vector<std::unique_ptr<KineticAutoConnection>>& connections)
{
  assert(values.size() <= operations.size());

  for (int opnum = 0; opnum < values.size(); opnum++) {
    if (operations[opnum].callback->getResult().statusCode() != StatusCode::OK) {

      auto handoff_key = make_shared<const string>(
          utility::Convert::toString("handoff=", *key, "version=", *version_new, "chunk=", opnum)
      );
      createSingleKey(handoff_key, version_new, values[opnum], connections);
    }
  }
}

StripeOperation_DEL::StripeOperation_DEL(const std::shared_ptr<const std::string>& key,
                                         const std::shared_ptr<const std::string>& version,
                                         kinetic::WriteMode writeMode,
                                         std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                                         std::size_t size, size_t offset) : StripeOperation(key)
{
  expandOperationVector(connections, size, offset);
  for (auto o = operations.begin(); o != operations.end(); o++) {
    auto cb = std::make_shared<DeleteCallback>(sync);
    o->callback = cb;
    o->function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
        const shared_ptr<const string>,
        const shared_ptr<const string>,
        WriteMode,
        const shared_ptr<SimpleCallbackInterface>,
        PersistMode)>(
        &ThreadsafeNonblockingKineticConnection::Delete,
        std::placeholders::_1,
        key,
        version,
        writeMode,
        cb,
        PersistMode::WRITE_BACK);
  }
}

kinetic::KineticStatus StripeOperation_DEL::execute(const std::chrono::seconds& timeout,
                                                    std::shared_ptr<RedundancyProvider>& redundancy)
{
  auto rmap = executeOperationVector(timeout);

  /* If we didn't find the key on a drive (e.g. because that drive was replaced)
    * we can just consider that key to be properly deleted on that drive. */
  if (rmap[StatusCode::OK] && rmap[StatusCode::REMOTE_NOT_FOUND]) {
    rmap[StatusCode::OK] += rmap[StatusCode::REMOTE_NOT_FOUND];
    rmap[StatusCode::REMOTE_NOT_FOUND] = 0;
  }

  /* Partial stripe remove has to be resolved */
  if (rmap[StatusCode::OK] && rmap[StatusCode::REMOTE_VERSION_MISMATCH]) {
    throw std::runtime_error("Partial stripe remove detected.");
  }

  for (auto it = rmap.cbegin(); it != rmap.cend(); it++) {
    if (it->second >= redundancy->numData()) {
      if (it->second < redundancy->size()) {
        need_indicator = true;
      }
      return KineticStatus(it->first, "");
    }
  }

  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Key " + *key + " not accessible.");
}


StripeOperation_GET::StripeOperation_GET(const std::shared_ptr<const std::string>& key, bool skip_value,
                                         std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                                         std::size_t size, size_t offset)
    : StripeOperation(key), skip_value(skip_value)
{
  expandOperationVector(connections, size, offset);
  fillOperationVector();
}

void StripeOperation_GET::extend(std::vector<std::unique_ptr<KineticAutoConnection>>& connections, std::size_t size)
{
  expandOperationVector(connections, size, operations.size());
  fillOperationVector();
}

void StripeOperation_GET::fillOperation(KineticAsyncOperation& op, std::shared_ptr<const std::string> key)
{
  if (skip_value) {
    auto cb = std::make_shared<GetVersionCallback>(sync);
    op.callback = cb;
    op.function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
        const std::shared_ptr<const std::string>,
        const shared_ptr<GetVersionCallbackInterface>)>(
        &ThreadsafeNonblockingKineticConnection::GetVersion,
        std::placeholders::_1,
        key,
        cb);
  }
  else {
    auto cb = std::make_shared<GetCallback>(sync);
    op.callback = cb;
    op.function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
        const std::shared_ptr<const std::string>,
        const shared_ptr<GetCallbackInterface>)>(
        &ThreadsafeNonblockingKineticConnection::Get,
        std::placeholders::_1,
        key,
        cb);
  }
}

void StripeOperation_GET::fillOperationVector()
{
  for (auto o = operations.begin(); o != operations.end(); o++) {
    if (o->callback) {
      continue;
    }
    fillOperation(*o, key);
  }
}


bool StripeOperation_GET::insertHandoffChunks(std::vector<std::unique_ptr<KineticAutoConnection>>& connections)
{
  /* If we don't even have a target version, there's no need to look for chunks */
  if(!version.version)
    return false;

  auto inserted = false;
  auto start_key = std::make_shared<const string>(
      utility::Convert::toString("handoff=", *key, "version=", *version.version)
  );
  auto end_key = std::make_shared<const string>(
      utility::Convert::toString("handoff=", *key, "version=", *version.version, "~")
  );

  ClusterRangeOp range(start_key, end_key, 100, connections);
  range.executeOperationVector(std::chrono::seconds(5));

  for (int i = 0; i < range.operations.size(); i++) {
    auto& opkeys = std::static_pointer_cast<RangeCallback>(range.operations[i].callback)->getKeys();
    kio_debug("found ", opkeys ? opkeys->size() : 0, " handoff keys on connection #", i);

    if (opkeys) {
      for (auto opkey = opkeys->begin(); opkey != opkeys->end(); opkey++) {
        std::size_t chunk_number = utility::Convert::toInt(opkey->substr(opkey->find_last_of("=") + 1));
        kio_debug("Chunk number is ", chunk_number);

        operations[chunk_number].connection = connections[i].get();
        fillOperation(operations[chunk_number], std::make_shared<const string>(*opkey));
        inserted = true;
      }
    }
  }
  return inserted;
}


void StripeOperation_GET::reconstructValue(std::shared_ptr<RedundancyProvider>& redundancy)
{
  value = make_shared<string>();

  auto size = (int) utility::uuidDecodeSize(version.version);
  if (!size) {
    kio_debug("Key ", *key, " is empty according to version: ", version.version);
    return;
  }
  std::vector<int> zeroed_indices;

  std::vector<shared_ptr<const string>> stripe;
  bool need_recovery = false;

  /* Step 1) re-construct stripe */
  for (int i = 0; i < operations.size(); i++) {
    auto& record = std::static_pointer_cast<GetCallback>(operations[i].callback)->getRecord();

    if (record && *record->version() == *version.version && record->value()) {
      auto checksum = crc32c(0, record->value()->c_str(), record->value()->length());
      auto tag = utility::Convert::toString(checksum);
      if (tag == *record->tag()) {
        stripe.push_back(record->value());
        /* If we have no value but passed crc verification, this indicates a 0ed data chunk has been used for
         * parity calculations but not unnecessarily written to the backend. */
        if (!record->value()->size()) {
          zeroed_indices.push_back(i);
        }
      }
      else {
        kio_warning("Chunk ", i, " of key ", *key, " failed crc verification.");
        stripe.push_back(make_shared<const string>());
        need_recovery = true;
        need_indicator = true;
      }
    }
    else {
      kio_notice("Chunk ", i, " of key ", *key, " is invalid.");
      stripe.push_back(make_shared<const string>());
      need_recovery = true;
    }
  }

  if (need_recovery) {

    if (zeroed_indices.size()) {
      int chunkSize = 0;
      for (auto it = stripe.begin(); it != stripe.end(); it++) {
        if ((*it)->length()) {
          chunkSize = (*it)->length();
          break;
        }
      }
      auto zero = std::make_shared<const std::string>(chunkSize, '\0');
      for (auto it = zeroed_indices.begin(); it != zeroed_indices.end(); it++) {
        assert(*it < redundancy->numData());
        assert(*it * chunkSize >= size);
        stripe[*it] = zero;
      }
    }
    redundancy->compute(stripe);
  }

  /* Step 2) merge data chunks into single value */
  value->reserve(size);
  for (auto it = stripe.cbegin(); it != stripe.cend(); it++) {
    if (value->size() + (*it)->size() <= size) {
      *value += **it;
    }
    else {
      value->append(**it, 0, size - value->size());
      break;
    }
  }
}

bool getVersionEqual(const std::shared_ptr<KineticCallback>& lhs, const std::shared_ptr<KineticCallback>& rhs)
{
  if (!lhs->getResult().ok() || !rhs->getResult().ok()) {
    return false;
  }

  return
      std::static_pointer_cast<GetVersionCallback>(lhs)->getVersion()
      ==
      std::static_pointer_cast<GetVersionCallback>(rhs)->getVersion();
}

bool getRecordVersionEqual(const std::shared_ptr<KineticCallback>& lhs, const std::shared_ptr<KineticCallback>& rhs)
{
  if (!lhs->getResult().ok() || !rhs->getResult().ok()) {
    return false;
  }

  return
      *std::static_pointer_cast<GetCallback>(lhs)->getRecord()->version()
      ==
      *std::static_pointer_cast<GetCallback>(rhs)->getRecord()->version();
}

StripeOperation_GET::VersionCount StripeOperation_GET::mostFrequentVersion()
{
  VersionCount v{std::shared_ptr<const std::string>(), 0};

  v.frequency = 0;

  auto element = operations.begin();
  for (auto o = operations.begin(); o != operations.end(); o++) {
    int frequency = 0;
    for (auto l = operations.begin(); l != operations.end(); l++) {
      if (skip_value ? getVersionEqual(o->callback, l->callback) : getRecordVersionEqual(o->callback, l->callback)) {
        frequency++;
      }
    }

    if (frequency > v.frequency) {
      v.frequency = frequency;
      element = o;
    }
    if (frequency > operations.size() / 2) {
      break;
    }
  }

  if (v.frequency) {
    if (skip_value) {
      v.version = std::make_shared<const std::string>(
          std::static_pointer_cast<GetVersionCallback>(element->callback)->getVersion());
    }
    else {
      v.version = std::static_pointer_cast<GetCallback>(element->callback)->getRecord()->version();
    }
  }
  return v;
}

kinetic::KineticStatus StripeOperation_GET::execute(const std::chrono::seconds& timeout,
                                                    std::shared_ptr<RedundancyProvider>& redundancy)
{
  auto rmap = executeOperationVector(timeout);
  version = mostFrequentVersion();

  /* Indicator required if chunk versions of this stripe are not aligned */
  if (rmap[StatusCode::OK] > version.frequency) {
    rmap[StatusCode::OK] = version.frequency;
    need_indicator = true;
  }

  /* Any status code encountered at least nData times is valid. If operation was a success, set return values. */
  for (auto it = rmap.begin(); it != rmap.end(); it++) {
    if (it->second >= redundancy->numData()) {
      if (it->first == kinetic::StatusCode::OK && !skip_value) {
        reconstructValue(redundancy);
      }
      if (validStatusCode(it->first)) {
        return KineticStatus(it->first, "");
      }
    }
  }
  throw std::runtime_error("No valid result.");
}

std::shared_ptr<const std::string> StripeOperation_GET::getVersion()
{
  return version.version;
}

std::shared_ptr<const std::string> StripeOperation_GET::getValue()
{
  return value;
}

size_t StripeOperation_GET::versionPosition(const std::shared_ptr<const std::string>& version) const
{
  for (int index = 0; index < operations.size(); index++) {
    auto& cb = operations[index].callback;

    if (version->empty() && cb->getResult().statusCode() == StatusCode::REMOTE_NOT_FOUND) {
      return index;
    }

    if (cb->getResult().ok()) {
      if (skip_value && *version == std::static_pointer_cast<GetVersionCallback>(cb)->getVersion()) {
        return index;
      }
      if (!skip_value && *version == *std::static_pointer_cast<GetCallback>(cb)->getRecord()->version()) {
        return index;
      }
    }
  }
  return operations.size();
}

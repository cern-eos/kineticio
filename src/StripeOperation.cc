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

/* Fire and forget put callback... not doing anything with the result. */
class FFPutCallback : public kinetic::PutCallbackInterface {
public:
  void Success()
  { }

  void Failure(kinetic::KineticStatus error)
  { }

  FFPutCallback()
  { }

  ~FFPutCallback()
  { }
};

void StripeOperation::putIndicatorKey(std::vector<std::unique_ptr<KineticAutoConnection>>& connections)
{
  /* Obtain an operation index where the execution succeeded. */
  int i = -1;
  while (i + 1 < operations.size()) {
    if (operations[++i].callback->getResult().statusCode() != StatusCode::CLIENT_IO_ERROR) {
      break;
    }
  }

  /* Set up record, version is set to "indicator" so that multiple attempts to write the same indicator key
   * fail with VERSION_MISMATCH instead of overwriting every single time. */
  auto record = std::make_shared<const KineticRecord>(
      "", "indicator", "", com::seagate::kinetic::client::proto::Command_Algorithm_INVALID_ALGORITHM
  );

  /* Schedule a write... we're not sticking around. If something fails, tough luck. */
  try {
    auto con = operations[i].connection->get();
    con->Put(utility::makeIndicatorKey(*key),
             make_shared<const string>(),
             kinetic::WriteMode::REQUIRE_SAME_VERSION,
             record,
             std::make_shared<FFPutCallback>());

    fd_set a;
    con->Run(&a, &a, &i);
  } catch (const std::exception& e) {
    kio_warning("Failed scheduling indication-key write for target-key ", *key, ": ", e.what());
  };
}

bool StripeOperation::needsIndicator()
{
  return need_indicator;
}


/*
void KineticCluster::putHandoffKeys(const std::shared_ptr<const std::string>& key,
                                    const std::shared_ptr<const std::string>& version,
                                    std::vector<std::shared_ptr<const std::string>>& values,
                                    const std::vector<KineticAsyncOperation>& ops)
{
  std::set<int> used_offsets;

  for (int opnum = 0; opnum < ops.size(); opnum++) {
    if (ops[opnum].callback->getResult().statusCode() == StatusCode::CLIENT_IO_ERROR) {

      for (auto offset = connections.size(); offset >= 0; offset--) {

        if (offset == opnum || used_offsets.count(offset)) {
          continue;
        }

        auto handoff_key = make_shared<const string>(
            utility::Convert::toString(*key, ":", *version, ":chunk=", opnum, ":offset=", offset)
        );
        std::vector<std::shared_ptr<const std::string>> handoff_value;
        handoff_value.push_back(values.at(opnum));
        auto handoff_op = initialize(key, 1, offset);
        auto sync = asyncops::fillPut(handoff_op, handoff_value, handoff_key, version, make_shared<const string>(),
                                      WriteMode::REQUIRE_SAME_VERSION);
        auto rmap = raw_execute(handoff_op, *sync);

        if (rmap[StatusCode::OK]) {
          kio_debug("Pushed handoff key for chunk ", opnum, " of key ", *key, " to drive with stripe offset ", offset);
          break;
        }
      }
    }
  }
}*/

StripeOperation_PUT::StripeOperation_PUT(const std::shared_ptr<const std::string>& key,
                                         const std::shared_ptr<const std::string>& version_new,
                                         const std::shared_ptr<const std::string>& version_old,
                                         std::vector<std::shared_ptr<const std::string>>& values,
                                         kinetic::WriteMode writeMode,
                                         std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                                         std::size_t size, size_t offset)
    : StripeOperation(key)
{
  expandOperationVector(connections, size, offset);
  for (int i = 0; i < operations.size(); i++) {
    auto v = values[i];

    /* Generate Checksum */
    auto checksum = crc32c(0, v->c_str(), v->length());
    auto tag = std::make_shared<string>(
        std::to_string((long long unsigned int) checksum)
    );

    /* Construct record structure. */
    auto record = std::make_shared<KineticRecord>(
        v, version_new, tag, com::seagate::kinetic::client::proto::Command_Algorithm_CRC32
    );

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
        std::cref(key),
        std::cref(version_old),
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
      if (it->first == StatusCode::OK) {
        if (it->second < redundancy->size()) {
          // TODO: putHandoffKeys(key, version_new, stripe, ops);
        }
      }
      if (it->second < redundancy->size()) {
        need_indicator = true;
      }
      return KineticStatus(it->first, "");
    }
  }
  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Key" + *key + "not accessible.");
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
        std::cref(key),
        std::cref(version),
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

void StripeOperation_GET::fillOperationVector()
{
  for (auto o = operations.begin(); o != operations.end(); o++) {
    if (o->callback) {
      continue;
    }
    if (skip_value) {
      auto cb = std::make_shared<GetVersionCallback>(sync);
      o->callback = cb;
      o->function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
          const std::shared_ptr<const std::string>,
          const shared_ptr<GetVersionCallbackInterface>)>(
          &ThreadsafeNonblockingKineticConnection::GetVersion,
          std::placeholders::_1,
          std::cref(key),
          cb);
    }
    else {
      auto cb = std::make_shared<GetCallback>(sync);
      o->callback = cb;
      o->function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
          const std::shared_ptr<const std::string>,
          const shared_ptr<GetCallbackInterface>)>(
          &ThreadsafeNonblockingKineticConnection::Get,
          std::placeholders::_1,
          std::cref(key),
          cb);
    }
  }
}

void StripeOperation_GET::reconstructValue(std::shared_ptr<RedundancyProvider>& redundancy, std::size_t chunkCapacity)
{
  value = make_shared<string>();

  auto size = (int) utility::uuidDecodeSize(version.version);
  if (!size) {
    kio_debug("Key ", *key, " is empty according to version: ",version.version);
    return;
  }
  auto chunkSize = size < chunkCapacity ? size : chunkCapacity;
  std::shared_ptr<std::string> zero;

  std::vector<shared_ptr<const string>> stripe;
  bool need_recovery = false;

  /* Step 1) re-construct stripe */
  for (int i = 0; i < operations.size(); i++) {
    auto& record = std::static_pointer_cast<GetCallback>(operations[i].callback)->getRecord();

    if (record && *record->version() == *version.version && record->value()) {
      auto checksum = crc32c(0, record->value()->c_str(), record->value()->length());
      auto tag = utility::Convert::toString(checksum);
      if (tag == *record->tag()) {
        if (redundancy->numData() == 1 || i * chunkSize < size) {
          stripe.push_back(record->value());
        }
        else {
          /* Add 0ed data chunks as necessary analogue to stripe creation */
          if (!zero) {
            zero = make_shared<string>();
            zero->resize(chunkSize);
          }
          stripe.push_back(zero);
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
    redundancy->compute(stripe);
  }

  /* Step 2) merge data chunks into single value */
  value->reserve(size);
  for (auto it = stripe.cbegin(); it != stripe.cend(); it++) {
    if (value->size() + chunkCapacity < size) {
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
                                                    std::shared_ptr<RedundancyProvider>& redundancy,
                                                    std::size_t chunkCapacity)
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
        reconstructValue(redundancy, chunkCapacity);
      }
      return KineticStatus(it->first, "");
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

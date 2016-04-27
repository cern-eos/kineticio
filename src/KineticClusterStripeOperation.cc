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
#include "KineticClusterStripeOperation.hh"
#include "outside/MurmurHash3.h"
#include <set>
#include <unistd.h>

using namespace kio;
using namespace kinetic;


KineticClusterStripeOperation::KineticClusterStripeOperation(
    std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
    const std::shared_ptr<const std::string>& key,
    std::shared_ptr<RedundancyProvider>& redundancy) :
    KineticClusterOperation(connections), key(key), need_indicator(false), redundancy(redundancy)
{
}

KineticClusterStripeOperation::~KineticClusterStripeOperation()
{

}

void KineticClusterStripeOperation::expandOperationVector(std::size_t size, std::size_t offset)
{
  uint32_t index;
  MurmurHash3_x86_32(key->c_str(), static_cast<uint32_t>(key->length()), 0, &index);
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
  return code == StatusCode::OK || code == StatusCode::REMOTE_NOT_FOUND ||
         code == StatusCode::REMOTE_VERSION_MISMATCH;
}

}

kinetic::KineticStatus KineticClusterStripeOperation::createSingleKey(
    std::shared_ptr<const std::string> keyname,
    std::shared_ptr<const std::string> keyversion,
    std::shared_ptr<const std::string> keyvalue)
{
  auto record = makeRecord(keyvalue, keyversion);
  size_t count = 0;

  do {
    expandOperationVector(1, operations.size());
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
  } while (!validStatusCode(operations.back().callback->getResult().statusCode()) && count < connections.size());

  kio_notice("Single key put ", *keyname, " with result ", operations.back().callback->getResult());
  return operations.back().callback->getResult();
}

void KineticClusterStripeOperation::putIndicatorKey()
{
  createSingleKey(utility::makeIndicatorKey(*key),
                  make_shared<const string>("indicator"),
                  make_shared<const string>()
  );
}

bool KineticClusterStripeOperation::needsIndicator()
{
  return need_indicator;
}

StripeOperation_PUT::StripeOperation_PUT(const std::shared_ptr<const std::string>& key,
                                         const std::shared_ptr<const std::string>& version_new,
                                         const std::shared_ptr<const std::string>& version_old,
                                         std::vector<std::shared_ptr<const std::string>>& values,
                                         kinetic::WriteMode writeMode,
                                         std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                                         std::shared_ptr<RedundancyProvider>& redundancy)
    : WriteStripeOperation(connections, key, redundancy), version_new(version_new), values(values)
{
  if (values.size() != redundancy->size()) {
    kio_error("Invalid input. Stripe of ", values.size(), " is not compatible with redundancy of ", redundancy->size());
    throw std::system_error(std::make_error_code(std::errc::invalid_argument));
  }
  expandOperationVector(values.size(), 0);
  for (size_t i = 0; i < operations.size(); i++) {
    fillOperation(i, version_old, writeMode);
  }
}

void StripeOperation_PUT::fillOperation(size_t index, const shared_ptr<const string>& drive_version,
                                        kinetic::WriteMode writeMode)
{

  auto record = makeRecord(values[index], version_new);
  auto cb = std::make_shared<PutCallback>(sync);

  operations[index].callback = cb;
  operations[index].function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
      const shared_ptr<const string>,
      const shared_ptr<const string>,
      WriteMode,
      const shared_ptr<const KineticRecord>,
      const shared_ptr<PutCallbackInterface>,
      PersistMode)>(
      &ThreadsafeNonblockingKineticConnection::Put,
      std::placeholders::_1,
      key,
      drive_version,
      writeMode,
      record,
      cb,
      PersistMode::WRITE_BACK);
}

kinetic::KineticStatus StripeOperation_PUT::execute(const std::chrono::seconds& timeout)
{
  auto rmap = executeOperationVector(timeout);

  /* Partial stripe write has to be resolved. */
  if (rmap[StatusCode::OK] && (rmap[StatusCode::REMOTE_VERSION_MISMATCH] || rmap[StatusCode::REMOTE_NOT_FOUND])) {
    resolvePartialWrite(timeout, version_new);
    /* re-compute results map */
    rmap.clear();
    for (auto it = operations.cbegin(); it != operations.cend(); it++) {
      rmap[it->callback->getResult().statusCode()]++;
    }
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

void StripeOperation_PUT::putHandoffKeys()
{
  for (size_t opnum = 0; opnum < values.size(); opnum++) {
    auto scode = operations[opnum].callback->getResult().statusCode();
    if (scode != StatusCode::OK && scode != StatusCode::REMOTE_VERSION_MISMATCH) {
      kio_debug("Creating handoff key due to status code ", scode, " on connection ",
                operations[opnum].connection->getName());
      auto handoff_key = make_shared<const string>(
          utility::Convert::toString("handoff=", *key, "version=", *version_new, "chunk=", opnum)
      );
      createSingleKey(handoff_key, version_new, values[opnum]);
    }
  }
}

WriteStripeOperation::WriteStripeOperation(
    std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
    const std::shared_ptr<const std::string>& key,
    std::shared_ptr<RedundancyProvider>& redundancy) : KineticClusterStripeOperation(connections, key, redundancy)
{
}


bool WriteStripeOperation::attemptStripeRepair(const std::chrono::seconds& timeout,
                                               const StripeOperation_GET& drive_versions)
{
  for (size_t opnum = 0; opnum < operations.size(); opnum++) {
    if (operations[opnum].callback->getResult().statusCode() == StatusCode::REMOTE_VERSION_MISMATCH) {
      auto drive_version = drive_versions.getVersionAt(opnum);
      if (drive_version) {
        fillOperation(opnum, drive_version, kinetic::WriteMode::REQUIRE_SAME_VERSION);
      }
    }
  }
  auto rmap = executeOperationVector(timeout);
  return rmap[StatusCode::REMOTE_VERSION_MISMATCH] == 0;
}

/* Resolve version missmatch errors due to concurrency
 * Three step algorithm
 *  1) -> update
 *  2) supplied version is not on backend at all ? ->
 *      a) initial write was successful -> has been updated by another client -> return ok
 *      b) initial write failed -> stripe has been repaired by another client -> return version missmatch
 *  3) supplied version is on backend but not most frequent -> wait for another client to update */
void WriteStripeOperation::resolvePartialWrite(const std::chrono::seconds& timeout,
                                               const std::shared_ptr<const std::string>& version)
{
  auto start_time = std::chrono::system_clock::now();
  auto position = 0;

  do {
    StripeOperation_GET getVersions(key, true, connections, redundancy, true);
    auto rmap = getVersions.executeOperationVector(timeout);
    auto most_frequent = getVersions.mostFrequentVersion();

    /* Remote not found does not reflect in most_frequent */
    if (rmap[StatusCode::REMOTE_NOT_FOUND] > most_frequent.frequency) {
      most_frequent.version = make_shared<const string>();
      most_frequent.frequency = rmap[StatusCode::REMOTE_NOT_FOUND];
    }

    /* 1) supplied version is most frequent version -> our turn to repair partial write */
    if (*version == *most_frequent.version) {
      if (attemptStripeRepair(timeout, getVersions)) {
        /* if repair is completely successful (there are no version missmatches left in stripe), we are done. */
        kio_debug("Stripe repair completed successfully for key ", *key);
        return;
      }
    }

    position = -1;
    for (size_t i = 0; i < redundancy->size(); i++) {
      auto drive_version = getVersions.getVersionAt(i);
      if ((drive_version && *version == *drive_version) || (!drive_version && version->empty())) {
        position = i;
        break;
      }
    }

    /* version is not in stripe at all. this situation can occur if a partial write has been fixed by another
     * client, or (assuming >= nData chunks of the version had been written initially), the stripe has
     * already been overwritten by another client.. nothing we need to do here, original evaluation */
    if (position < 0) {
      kio_debug("Stripe for key ", *key, " has been completely overwritten by someone else. Aborting.");
      return;
    }

    /* version is in the stripe, but is not the most frequent version.
     * It could be that the client that should win the most_frequent match has crashed. For this
     * reason, all competing clients will wait, polling the key versions. As multiple clients
     * could theoretically be in this loop concurrently, we specify the maximum number of polls by the position of the first
     * occurrence of the supplied version for a chunk. If the situation does not clear up by end of polling period,
     * overwrite permission will be given. */
    usleep(100 * 1000);
  } while (std::chrono::system_clock::now() < start_time + timeout * (position + 1));

  kio_warning("Client crash detected. Overwriting with version ", *version);
  StripeOperation_GET getVersions(key, true, connections, redundancy, true);
  getVersions.executeOperationVector(timeout);
  if (!attemptStripeRepair(timeout, getVersions)) {
    kio_warning("Failed repairing stripe.");
  }

}

StripeOperation_DEL::StripeOperation_DEL(const std::shared_ptr<const std::string>& key,
                                         const std::shared_ptr<const std::string>& version,
                                         kinetic::WriteMode writeMode,
                                         std::vector<std::unique_ptr<KineticAutoConnection>>& connections,
                                         std::shared_ptr<RedundancyProvider>& redundancy,
                                         std::size_t size, size_t offset) :
    WriteStripeOperation(connections, key, redundancy)
{
  expandOperationVector(size, offset);
  for (size_t i = 0; i < operations.size(); i++) {
    fillOperation(i, version, writeMode);
  }
}

void StripeOperation_DEL::fillOperation(size_t index, const std::shared_ptr<const std::string>& drive_version,
                                        kinetic::WriteMode writeMode)
{
  auto cb = std::make_shared<DeleteCallback>(sync);
  operations[index].callback = cb;
  operations[index].function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
      const shared_ptr<const string>,
      const shared_ptr<const string>,
      WriteMode,
      const shared_ptr<SimpleCallbackInterface>,
      PersistMode)>(
      &ThreadsafeNonblockingKineticConnection::Delete,
      std::placeholders::_1,
      key,
      drive_version,
      writeMode,
      cb,
      PersistMode::WRITE_BACK);
}

kinetic::KineticStatus StripeOperation_DEL::execute(const std::chrono::seconds& timeout)
{
  auto rmap = executeOperationVector(timeout);

  /* Partial stripe remove has to be resolved */
  if (rmap[StatusCode::OK] && rmap[StatusCode::REMOTE_VERSION_MISMATCH]) {
    auto empty_version = make_shared<const string>();
    resolvePartialWrite(timeout, empty_version);
    /* re-compute results map */
    rmap.clear();
    for (auto it = operations.cbegin(); it != operations.cend(); it++) {
      rmap[it->callback->getResult().statusCode()]++;
    }
  }

  /* If we didn't find the key on a drive (e.g. because that drive was replaced)
  * we can just consider that key to be properly deleted on that drive. */
  if (rmap[StatusCode::OK] && rmap[StatusCode::REMOTE_NOT_FOUND]) {
    rmap[StatusCode::OK] += rmap[StatusCode::REMOTE_NOT_FOUND];
    rmap[StatusCode::REMOTE_NOT_FOUND] = 0;
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
                                         std::shared_ptr<RedundancyProvider>& redundancy, bool skip_partial_get)
    : KineticClusterStripeOperation(connections, key, redundancy), skip_value(skip_value)
{
  if (skip_partial_get) {
    expandOperationVector(redundancy->size(), 0);
  }
  else {
    expandOperationVector(redundancy->numData(), 0);
  }
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


bool StripeOperation_GET::insertHandoffChunks()
{
  /* If we don't even have a target version, there's no need to look for chunks */
  if (!version.version) {
    return false;
  }

  auto inserted = false;
  auto start_key = std::make_shared<const string>(
      utility::Convert::toString("handoff=", *key, "version=", *version.version)
  );
  auto end_key = std::make_shared<const string>(
      utility::Convert::toString("handoff=", *key, "version=", *version.version, "~")
  );

  ClusterRangeOp range(start_key, end_key, 100, connections);
  range.executeOperationVector(std::chrono::seconds(5));

  for (size_t i = 0; i < range.operations.size(); i++) {
    auto& opkeys = std::static_pointer_cast<RangeCallback>(range.operations[i].callback)->getKeys();
    kio_debug("found ", opkeys ? opkeys->size() : 0, " handoff keys on connection #", i);

    if (opkeys) {
      for (auto opkey = opkeys->begin(); opkey != opkeys->end(); opkey++) {
        auto chunk_number = utility::Convert::toInt(opkey->substr(opkey->find_last_of("=") + 1));
        kio_debug("Chunk number is ", chunk_number);

        operations[chunk_number].connection = connections[i].get();
        fillOperation(operations[chunk_number], std::make_shared<const string>(*opkey));
        inserted = true;
      }
    }
  }
  return inserted;
}


void StripeOperation_GET::reconstructValue()
{
  value = make_shared<string>();

  auto size = utility::uuidDecodeSize(version.version);
  if (!size) {
    kio_debug("Key ", *key, " is empty according to version: ", version.version);
    return;
  }
  std::vector<size_t> zeroed_indices;

  std::vector<shared_ptr<const string>> stripe;
  bool need_recovery = false;

  /* Step 1) re-construct stripe */
  for (size_t i = 0; i < operations.size(); i++) {
    auto& record = std::static_pointer_cast<GetCallback>(operations[i].callback)->getRecord();

    if (record && *record->version() == *version.version && record->value()) {
      auto checksum = crc32c(0, record->value()->c_str(), record->value()->length());
      auto tag = utility::Convert::toString(checksum);
      if (tag == *record->tag()) {
        stripe.push_back(record->value());
        /* If we have no value but passed crc verification, this indicates a 0ed data chunk has been used for
         * parity calculations but not unnecessarily written to the backend. */
        if (!record->value()->size() && i < redundancy->numData()) {
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
      if (record && *record->version() != *version.version) {
        kio_notice("Chunk ", i, " of key ", *key, " has incorrect version. "
            "Expected = ", *version.version, "    Observed = ", *record->version());
      }
      else {
        kio_notice("Chunk ", i, " of key ", *key, " is invalid.");
      }
      stripe.push_back(make_shared<const string>());
      need_recovery = true;
    }
  }

  if (need_recovery) {

    if (zeroed_indices.size()) {
      size_t chunkSize = 0;
      for (auto it = stripe.cbegin(); it != stripe.cend(); it++) {
        if ((*it)->length()) {
          chunkSize = (*it)->length();
          break;
        }
      }
      auto zero = std::make_shared<const std::string>(chunkSize, '\0');
      for (auto it = zeroed_indices.cbegin(); it != zeroed_indices.cend(); it++) {
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

StripeOperation_GET::VersionCount StripeOperation_GET::mostFrequentVersion() const
{
  VersionCount v{std::shared_ptr<const std::string>(), 0};

  v.frequency = 0;

  auto element = operations.cbegin();
  for (auto o = operations.cbegin(); o != operations.cend(); o++) {
    size_t frequency = 0;
    for (auto l = operations.cbegin(); l != operations.cend(); l++) {
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


kinetic::KineticStatus StripeOperation_GET::do_execute(const std::chrono::seconds& timeout)
{
  auto rmap = executeOperationVector(timeout);
  version = mostFrequentVersion();

  /* Indicator required if chunk versions of this stripe are not aligned */
  if (rmap[StatusCode::OK] > version.frequency) {
    rmap[StatusCode::OK] = version.frequency;
    need_indicator = true;
  }

  /* Any status code encountered at least nData times is valid. If operation was a success, set return values. */
  for (auto it = rmap.cbegin(); it != rmap.cend(); it++) {
    if (it->second >= redundancy->numData()) {
      if (it->first == kinetic::StatusCode::OK && !skip_value) {
        reconstructValue();
      }
      if (validStatusCode(it->first)) {
        return KineticStatus(it->first, "");
      }
    }
  }
  throw std::runtime_error("No valid result.");
}


kinetic::KineticStatus StripeOperation_GET::execute(const std::chrono::seconds& timeout)
{
  /* Skip attempting to read without parities for replicated keys. Version verification is required to validate result */
  if (redundancy->numData() > 1) {
    try {
      return do_execute(timeout);
    } catch (std::exception& e) {
      kio_debug("Failed getting stripe for key ", *key, " without parities: ", e.what());
    }
  }

  /* Add parity chunks to get request (already obtained chunks will not be re-fetched). */
  if (operations.size() == redundancy->numData()) {
    expandOperationVector(redundancy->numParity(), operations.size());
    fillOperationVector();
  }
  try {
    return do_execute(timeout);
  } catch (std::exception& e) {
    kio_debug("Failed getting stripe for key ", *key, " even with parities: ", e.what());
  }

  /* As a last ditch effort try to use handoff chunks if any are available to serve the request */
  if (this->insertHandoffChunks()) {
    try {
      return do_execute(timeout);
    } catch (std::exception& e) {
      kio_debug("Failed getting stripe for key ", *key, " even with handoff chunks: ", e.what());
    }
  }
  return KineticStatus(StatusCode::CLIENT_IO_ERROR, "Key " + *key + " not accessible.");
}

std::shared_ptr<const std::string> StripeOperation_GET::getVersion() const
{
  return version.version;
}

std::shared_ptr<const std::string> StripeOperation_GET::getValue() const
{
  return value;
}

std::shared_ptr<const std::string> StripeOperation_GET::getVersionAt(int index) const
{
  auto& cb = operations[index].callback;

  if (cb->getResult().statusCode() == StatusCode::REMOTE_NOT_FOUND) {
    return make_shared<const string>();
  }

  if (cb->getResult().ok()) {
    if (skip_value) {
      return make_shared<const std::string>(std::static_pointer_cast<GetVersionCallback>(cb)->getVersion());
    }
    else {
      return std::static_pointer_cast<GetCallback>(cb)->getRecord()->version();
    }
  }

  return std::shared_ptr<const std::string>();
}

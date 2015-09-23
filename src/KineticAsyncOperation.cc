#include "KineticAsyncOperation.hh"
#include "Utility.hh"

using namespace kio;
using namespace kinetic;

unique_ptr<CallbackSynchronization> asyncops::fillGetVersion(
    std::vector<KineticAsyncOperation>& ops,
    const std::shared_ptr<const std::string>& key
)
{
  auto sync = std::unique_ptr<CallbackSynchronization>(new CallbackSynchronization());
  for (auto o = ops.begin(); o != ops.end(); o++) {
    auto cb = std::make_shared<GetVersionCallback>(*sync);
    o->callback = cb;
    o->function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
        const std::shared_ptr<const std::string>,
        const shared_ptr<GetVersionCallbackInterface>)>(
        &ThreadsafeNonblockingKineticConnection::GetVersion,
        std::placeholders::_1,
        std::cref(key),
        cb);
  }
  return sync;
}

unique_ptr<CallbackSynchronization> asyncops::fillGet(
    std::vector<KineticAsyncOperation>& ops,
    const std::shared_ptr<const std::string>& key
)
{
  auto sync = std::unique_ptr<CallbackSynchronization>(new CallbackSynchronization());
  for (auto o = ops.begin(); o != ops.end(); o++) {
    auto cb = std::make_shared<GetCallback>(*sync);
    o->callback = cb;
    o->function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
        const std::shared_ptr<const std::string>,
        const shared_ptr<GetCallbackInterface>)>(
        &ThreadsafeNonblockingKineticConnection::Get,
        std::placeholders::_1,
        std::cref(key),
        cb);
  }
  return sync;
}


unique_ptr<CallbackSynchronization> asyncops::fillPut(
    std::vector<KineticAsyncOperation>& ops,
    std::vector<shared_ptr<const string> >& stripe,
    const std::shared_ptr<const std::string>& key,
    const shared_ptr<const string>& version_new,
    const std::shared_ptr<const string>& version_old,
    WriteMode wmode
)
{
  auto sync = std::unique_ptr<CallbackSynchronization>(new CallbackSynchronization());

  for (int i = 0; i < ops.size(); i++) {
    auto &v = stripe[i];

    /* Generate Checksum, computing takes ~1ms per checksum */
    auto checksum = crc32c(0, v->c_str(), v->length());
    auto tag = std::make_shared<string>(
        std::to_string((long long unsigned int) checksum)
    );

    /* Construct record structure. */
    shared_ptr<KineticRecord> record(
        new KineticRecord(v, version_new, tag,
                          com::seagate::kinetic::client::proto::Command_Algorithm_CRC32)
    );

    auto cb = std::make_shared<PutCallback>(*sync);
    ops[i].callback = cb;
    ops[i].function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
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
        wmode,
        record,
        cb,
        PersistMode::WRITE_BACK);
  }
  return sync;
}


unique_ptr<CallbackSynchronization> asyncops::fillRemove(
    std::vector<KineticAsyncOperation>& ops,
    const shared_ptr<const string>& key,
    const shared_ptr<const string>& version,
    WriteMode wmode
)
{
  auto sync = std::unique_ptr<CallbackSynchronization>(new CallbackSynchronization());
  for (auto o = ops.begin(); o != ops.end(); o++) {
    auto cb = std::make_shared<DeleteCallback>(*sync);
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
        wmode,
        cb,
        PersistMode::WRITE_BACK);
  }
  return sync;
}

unique_ptr<CallbackSynchronization> asyncops::fillRange(
    std::vector<KineticAsyncOperation>& ops,
    const std::shared_ptr<const std::string>& start_key,
    const std::shared_ptr<const std::string>& end_key,
    int maxRequested
)
{
  auto sync = std::unique_ptr<CallbackSynchronization>(new CallbackSynchronization());
  for (auto o = ops.begin(); o != ops.end(); o++) {
    auto cb = std::make_shared<RangeCallback>(*sync);
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
        maxRequested,
        cb);
  }
  return sync;
}


unique_ptr<CallbackSynchronization> asyncops::fillLog(
    std::vector<KineticAsyncOperation>& ops,
    const std::vector<Command_GetLog_Type> types
)
{
  auto sync = std::unique_ptr<CallbackSynchronization>(new CallbackSynchronization());
  for (auto o = ops.begin(); o != ops.end(); o++) {
    auto cb = std::make_shared<GetLogCallback>(*sync);
    o->callback = cb;
    o->function = std::bind<HandlerKey(ThreadsafeNonblockingKineticConnection::*)(
        const vector<Command_GetLog_Type> &,
        const shared_ptr<GetLogCallbackInterface>)>(
        &ThreadsafeNonblockingKineticConnection::GetLog,
        std::placeholders::_1,
        types,
        cb);
  }
  return sync;
}



/* return the frequency of the most common element in the vector, as well
   as a reference to that element. */
KineticAsyncOperation& mostFrequent(
    std::vector<KineticAsyncOperation>& ops, int& count,
    std::function<bool(const KineticAsyncOperation&, const KineticAsyncOperation&)> equal
)
{
  count = 0;
  auto element = ops.begin();

  for (auto o = ops.begin(); o != ops.end(); o++) {
    int frequency = 0;
    for (auto l = ops.begin(); l != ops.end(); l++)
      if (equal(*o, *l)) {
        frequency++;
      }

    if (frequency > count) {
      count = frequency;
      element = o;
    }
    if (frequency > ops.size() / 2) {
      break;
    }
  }
  return *element;
}

bool resultsEqual(const KineticAsyncOperation& lhs, const KineticAsyncOperation& rhs)
{
  return lhs.callback->getResult().statusCode() == rhs.callback->getResult().statusCode();
}

bool getVersionEqual(const KineticAsyncOperation& lhs, const KineticAsyncOperation& rhs)
{
  if((!lhs.callback->getResult().ok() && lhs.callback->getResult().statusCode() != StatusCode::REMOTE_NOT_FOUND) ||
     (!rhs.callback->getResult().ok() && rhs.callback->getResult().statusCode() != StatusCode::REMOTE_NOT_FOUND))
    return false;

  return
      std::static_pointer_cast<GetVersionCallback>(lhs.callback)->getVersion()
      ==
      std::static_pointer_cast<GetVersionCallback>(rhs.callback)->getVersion();
}

bool getRecordVersionEqual(const KineticAsyncOperation& lhs, const KineticAsyncOperation& rhs)
{
  if (!std::static_pointer_cast<GetCallback>(lhs.callback)->getRecord() ||
      !std::static_pointer_cast<GetCallback>(rhs.callback)->getRecord())
    return false;

  return
      *std::static_pointer_cast<GetCallback>(lhs.callback)->getRecord()->version()
      ==
      *std::static_pointer_cast<GetCallback>(rhs.callback)->getRecord()->version();
}

asyncops::VersionCount asyncops::mostFrequentRecordVersion(std::vector<KineticAsyncOperation>& ops)
{
  asyncops::VersionCount v{std::shared_ptr<const std::string>(), 0};
  auto& op = mostFrequent(ops, v.frequency, getRecordVersionEqual);
  v.version = std::static_pointer_cast<GetCallback>(op.callback)->getRecord()->version();
  return v;
}

asyncops::VersionCount asyncops::mostFrequentVersion(std::vector<KineticAsyncOperation>& ops)
{
  asyncops::VersionCount v{std::shared_ptr<const std::string>(), 0};
  auto& op = mostFrequent(ops, v.frequency, getVersionEqual);
  v.version = std::make_shared<const std::string>(
      std::static_pointer_cast<GetVersionCallback>(op.callback)->getVersion()
  );
  return v;

}
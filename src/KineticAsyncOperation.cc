#include "KineticAsyncOperation.hh"
#include <zlib.h>

using namespace kio;
using namespace kinetic;

std::shared_ptr<CallbackSynchronization> asyncop_fill::getVersion(
    std::vector<KineticAsyncOperation> &ops,
    const std::shared_ptr<const std::string> &key
)
{
  auto sync = std::make_shared<CallbackSynchronization>();
  for (auto o = ops.begin(); o != ops.end(); o++) {
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
  return sync;
}

std::shared_ptr<CallbackSynchronization> asyncop_fill::get(
    std::vector<KineticAsyncOperation> &ops,
    const std::shared_ptr<const std::string> &key
)
{
  auto sync = std::make_shared<CallbackSynchronization>();
  for (auto o = ops.begin(); o != ops.end(); o++) {
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
  return sync;
}


std::shared_ptr<CallbackSynchronization> asyncop_fill::put(
    std::vector<KineticAsyncOperation> &ops,
    std::vector<shared_ptr<const string> > &stripe,
    const std::shared_ptr<const std::string> &key,
    const shared_ptr<const string> &version_new,
    const std::shared_ptr<const string> &version_old,
    WriteMode wmode
)
{
  auto sync = std::make_shared<CallbackSynchronization>();

  for (int i = 0; i < ops.size(); i++) {
    auto &v = stripe[i];

    /* Generate Checksum, computing takes ~1ms per checksum */
    auto checksum = crc32(0, (const Bytef *) v->c_str(), v->length());
    auto tag = std::make_shared<string>(
        std::to_string((long long unsigned int) checksum)
    );

    /* Construct record structure. */
    shared_ptr<KineticRecord> record(
        new KineticRecord(v, version_new, tag,
                          com::seagate::kinetic::client::proto::Command_Algorithm_CRC32)
    );

    auto cb = std::make_shared<PutCallback>(sync);
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


std::shared_ptr<CallbackSynchronization> asyncop_fill::remove(
    std::vector<KineticAsyncOperation> &ops,
    const shared_ptr<const string> &key,
    const shared_ptr<const string> &version,
    WriteMode wmode
)
{
  auto sync = std::make_shared<CallbackSynchronization>();
  for (auto o = ops.begin(); o != ops.end(); o++) {
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
        wmode,
        cb,
        PersistMode::WRITE_BACK);
  }
  return sync;
}

std::shared_ptr<CallbackSynchronization> asyncop_fill::range(
    std::vector<KineticAsyncOperation> &ops,
    const std::shared_ptr<const std::string> &start_key,
    const std::shared_ptr<const std::string> &end_key,
    int maxRequested
)
{
  auto sync = std::make_shared<CallbackSynchronization>();
  for (auto o = ops.begin(); o != ops.end(); o++) {
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
        maxRequested,
        cb);
  }
  return sync;
}


std::shared_ptr<CallbackSynchronization> asyncop_fill::log(
    std::vector<KineticAsyncOperation> &ops,
    const std::vector<Command_GetLog_Type> types
)
{
  auto sync = std::make_shared<CallbackSynchronization>();
  for (auto o = ops.begin(); o != ops.end(); o++) {
    auto cb = std::make_shared<GetLogCallback>(sync);
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
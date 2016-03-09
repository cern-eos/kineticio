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

#include "KineticAutoConnection.hh"
#include "KineticIoSingleton.hh"
#include <sstream>
#include <Logging.hh>

using namespace kinetic;
using namespace kio;

KineticAutoConnection::KineticAutoConnection(
    SocketListener& sw,
    std::pair<kinetic::ConnectionOptions, kinetic::ConnectionOptions> o,
    std::chrono::seconds r) :
    options(o), ratelimit(r), connection(), healthy(false), fd(0), timestamp(std::chrono::system_clock::now()),
    mutex(), sockwatch(sw), mt(), bg(1, 0)
{
  std::random_device rd;
  mt.seed(rd());
  logstring = utility::Convert::toString(
      "(", options.first.host, ":", options.first.port, " / ", options.second.host, ":", options.second.port, ")"
  );
}

KineticAutoConnection::~KineticAutoConnection()
{
  if (fd) {
    sockwatch.unsubscribe(fd);
  }
}

const std::string& KineticAutoConnection::getName() const
{
  return logstring;
}

void KineticAutoConnection::setError(std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection>& errorConnection)
{
  std::lock_guard<std::mutex> lock(mutex);
  if (!healthy) {
    return;
  }

  if (connection != errorConnection) {
    kio_debug("Disregarding setError on ", getName(), " as underlying connection does not match the connection that "
        "showed an error. This indicates that a reconnect attempt has been succesffully completed in the meantime.");
    return;
  }

  if (fd) {
    sockwatch.unsubscribe(fd);
    fd = 0;
  }
  kio_notice("Setting connection ", getName(), " into error state.");
  healthy = false;
}

std::shared_ptr<kinetic::ThreadsafeNonblockingKineticConnection> KineticAutoConnection::get()
{
  std::call_once(intial_connect, &KineticAutoConnection::connect, this);

  std::lock_guard<std::mutex> lock(mutex);
  if (healthy) {
    return connection;
  }

  /* Rate limit re-connection attempts. */
  using namespace std::chrono;
  auto duration = duration_cast<seconds>(std::chrono::system_clock::now() - timestamp);
  if (duration > ratelimit) {
    if (bg.try_run(std::bind(&KineticAutoConnection::connect, this))) {
      timestamp = std::chrono::system_clock::now();
      kio_debug(logstring, " Scheduled background reconnect. Last reconnect attempt has been scheduled ",
                duration, " ago. ratelimit is ", ratelimit);
    }
    else {
      kio_notice(logstring, " Failed scheduling background reconnect despite last having been scheduled ",
                 duration, " ago. ratelimit is ", ratelimit);
    }
  }
  throw std::system_error(std::make_error_code(std::errc::not_connected));
}

namespace {
class ConnectCallback : public kinetic::SimpleCallbackInterface {
public:
  void Success()
  { _done = _success = true; }

  void Failure(kinetic::KineticStatus error)
  { _done = true; }

  ConnectCallback() : _done(false), _success(false)
  { }

  ~ConnectCallback()
  { }

  bool done()
  { return _done; }

  bool ok()
  { return _success; }

private:
  bool _done;
  bool _success;
};
}

void KineticAutoConnection::connect()
{
  kio_debug("Starting connection attempt", logstring);

  /* Choose connection to prioritize at random. */
  auto r = mt() % 2;
  auto& primary = r ? options.first : options.second;
  auto& secondary = r ? options.second : options.first;

  auto tmpfd = 0;
  std::shared_ptr<ThreadsafeNonblockingKineticConnection> tmpcon;
  KineticConnectionFactory factory = NewKineticConnectionFactory();

  if (factory.NewThreadsafeNonblockingConnection(primary, tmpcon).ok() ||
      factory.NewThreadsafeNonblockingConnection(secondary, tmpcon).ok()) {
    auto cb = std::make_shared<ConnectCallback>();
    tmpcon->NoOp(cb);

    /* Get the fd */
    fd_set x;
    int y;
    tmpcon->Run(&x, &x, &tmpfd);

    /* wait on noop reply... we actually don't want to add drives where the connection succeeds
     * but requests don't (e.g. drive is in locked state or has an error) */
    while (tmpcon->Run(&x, &x, &y) && !cb->done()) {
      struct timeval tv{5, 0};
      if (select(tmpfd, &x, &x, NULL, &tv) <= 0) {
        break;
      }
    }
    if (!cb->ok()) {
      tmpfd = 0;
    }
  }
  else {
    kio_debug("Factory did not return a connection. ", logstring);
  }

  {
    std::lock_guard<std::mutex> lock(mutex);
    if (tmpfd) {
      tmpfd--;
      sockwatch.subscribe(tmpfd, this);
      fd = tmpfd;
      connection = std::move(tmpcon);
      healthy = true;
      timestamp = std::chrono::system_clock::now();
      kio_debug("Connection attempt succeeded ", logstring);
    }
    else {
      kio_debug("Connection attempt failed ", logstring);
    }
  }
}

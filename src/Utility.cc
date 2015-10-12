#include "Utility.hh"
#include <iomanip>
#include <uuid/uuid.h>

using namespace kio;

static std::string toString(const kinetic::StatusCode& c)
{
  using kinetic::StatusCode;
  switch(c){
    case StatusCode::OK:
      return "OK";
    case StatusCode::CLIENT_IO_ERROR:
      return "CLIENT_IO_ERROR";
    case StatusCode::CLIENT_SHUTDOWN:
      return "CLIENT_SHUTDOWN";
    case StatusCode::CLIENT_INTERNAL_ERROR:
      return "CLIENT_INTERNAL_ERROR";
    case StatusCode::CLIENT_RESPONSE_HMAC_VERIFICATION_ERROR:
      return "CLIENT_RESPONSE_HMAC_VERIFICATION_ERROR";
    case StatusCode::REMOTE_HMAC_ERROR:
      return "REMOTE_HMAC_ERROR";
    case StatusCode::REMOTE_NOT_AUTHORIZED:
      return "REMOTE_NOT_AUTHORIZED";
    case StatusCode::REMOTE_CLUSTER_VERSION_MISMATCH:
      return "REMOTE_CLUSTER_VERSION_MISMATCH";
    case StatusCode::REMOTE_NOT_FOUND:
      return "REMOTE_NOT_FOUND";
    case StatusCode::REMOTE_VERSION_MISMATCH:
      return "REMOTE_VERSION_MISMATCH";
    default:
      return "OTHER_ERROR (code == " + std::to_string((long long int) static_cast<int>(c)) + ")";
  }
}

std::ostream& kio::utility::operator<<(std::ostream& os, const kinetic::StatusCode& c)
{
  os << toString(c);
  return os;
}

std::ostream& kio::utility::operator<<(std::ostream& os, const kinetic::KineticStatus& s)
{
  os << s.statusCode() << ": " << s.message();
  return os;
}

std::ostream& kio::utility::operator<<(std::ostream& os, const std::chrono::seconds& s)
{
  os << s.count() << " seconds";
  return os;
}


std::shared_ptr<const std::string> utility::constructBlockKey(const std::string& base, int block_number)
{
  std::ostringstream ss;
  ss << base << "_" << std::setw(10) << std::setfill('0') << block_number;
  return std::make_shared<const std::string>(ss.str());
}

std::string utility::extractClusterID(const std::string& path)
{
  size_t id_start = path.find_first_of(':') + 1;
  size_t id_end   = path.find_first_of(':', id_start);
  return path.substr(id_start, id_end-id_start);
}

std::shared_ptr<const std::string> utility::uuidGenerateEncodeSize(std::size_t size)
{
  uuid_t uuid;
  uuid_generate(uuid);

  std::ostringstream ss;
  ss << std::setw(10) << std::setfill('0') << size;

  return std::make_shared<const std::string>(
      ss.str() + std::string(reinterpret_cast<const char *>(uuid), sizeof(uuid_t))
   );
}

std::size_t utility::uuidDecodeSize(const std::shared_ptr<const std::string>& uuid)
{
  if(!uuid || uuid->size() != 10 + sizeof(uuid_t))
    throw std::invalid_argument("invalid version supplied.");
  std::string size(uuid->substr(0,10));
  return atoi(size.c_str());
}

std::shared_ptr<const std::string> utility::keyToIndicator(const std::string& key)
{
  return std::make_shared<const std::string>("-indicator-" + key);
}

std::shared_ptr<const std::string> utility::indicatorToKey(const std::string& indicator_key)
{
  return std::make_shared<const std::string>(indicator_key.substr(sizeof("-indicator"),std::string::npos));  
}

std::shared_ptr<const std::string> utility::constructAttributeKey(const std::string& key, const std::string& attribute_name)
{
  return std::make_shared<const std::string>("-attribute-"+ key + "-" + attribute_name);
}
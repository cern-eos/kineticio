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

std::string utility::extractClusterID(const std::string& path)
{
  size_t id_start = path.find_first_of(':') + 1;
  size_t id_end   = path.find_first_of(':', id_start);
  return path.substr(id_start, id_end-id_start);
}

std::string utility::extractBasePath(const std::string& path)
{
  auto start = path.find_first_of(':') + 1;
  return path.substr(path.find_first_of(':', start) + 1);
}

std::string utility::uuidGenerateString()
{
  uuid_t uuid;
  uuid_generate(uuid);
  return std::string(reinterpret_cast<const char *>(uuid), sizeof(uuid_t));
}

std::shared_ptr<const std::string> utility::uuidGenerateEncodeSize(std::size_t size)
{
  std::ostringstream ss;
  ss << std::setw(10) << std::setfill('0') << size;
  return std::make_shared<const std::string>(ss.str() + uuidGenerateString());
}

std::size_t utility::uuidDecodeSize(const std::shared_ptr<const std::string>& uuid)
{
  if(!uuid || uuid->size() != 10 + sizeof(uuid_t))
    throw std::invalid_argument("invalid version supplied.");
  std::string size(uuid->substr(0,10));
  return (size_t) atoi(size.c_str());
}

std::shared_ptr<const std::string> utility::makeDataKey(const std::string& clusterId, const std::string& base, int block_number)
{
  std::ostringstream ss;
  ss << clusterId << ":data:" << base << "_" << std::setw(10) << std::setfill('0') << block_number;
  return std::make_shared<const std::string>(ss.str());
}

std::shared_ptr<const std::string> utility::makeMetadataKey(const std::string& clusterId, const std::string& base)
{
  return std::make_shared<const std::string>(clusterId + ":metadata:" + base);
}

std::shared_ptr<const std::string> utility::makeAttributeKey(const std::string& clusterId, const std::string& base, const std::string& attribute_name)
{
  return std::make_shared<const std::string>(clusterId + ":attribute:"+ base + ":" + attribute_name);
}

std::string utility::extractAttributeName(const std::string& attrkey)
{
  auto pos = 0;
  for(int i=0; i<3; i++)
    attrkey.find_first_of(':',pos+1);
  return attrkey.substr(pos);
}

std::shared_ptr<const std::string> utility::makeIndicatorKey(const std::string& key)
{
  return std::make_shared<const std::string>("indicator:" + key);
}

std::shared_ptr<const std::string> utility::indicatorToKey(const std::string& indicator_key)
{
  return std::make_shared<const std::string>(indicator_key.substr(sizeof("indicator"),std::string::npos));  
}

std::string utility::metadataToPath(const std::string& mdkey)
{
  auto pos1 = mdkey.find_first_of(':');
  auto pos2 = mdkey.find_first_of(':', pos1+1);  
  return "kinetic:" + mdkey.substr(0,pos1) + mdkey.substr(pos2);
}
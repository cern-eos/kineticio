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

#include "Utility.hh"
#include <iomanip>
#include <uuid/uuid.h>

using namespace kio;

static std::string toString(const kinetic::StatusCode& c)
{
  using kinetic::StatusCode;
  switch (c) {
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

std::string utility::urlToClusterId(const std::string& url)
{
  size_t id_start = strlen("kinetic://");
  size_t id_end = url.find_first_of('/', id_start);
  return url.substr(id_start, id_end - id_start);
}

std::string utility::urlToPath(const std::string& url)
{
  return url.substr(url.find_first_of('/', strlen("kinetic://") + 1) + 1);
}

std::string utility::metadataToUrl(const std::string& mdkey)
{
  auto pos1 = mdkey.find_first_of(':');
  auto pos2 = mdkey.find_first_of(':', pos1 + 1);
  return "kinetic://" + mdkey.substr(0, pos1) + "/" + mdkey.substr(pos2 + 1);
}

std::string utility::uuidGenerateString()
{
  uuid_t uuid;
  uuid_generate(uuid);

  char uuid_str[37];  // 36 byte string plus "\0"
  uuid_unparse_lower(uuid, uuid_str);

  return std::string(uuid_str);
}

std::shared_ptr<const std::string> utility::uuidGenerateEncodeSize(std::size_t size)
{
  std::ostringstream ss;
  ss << std::setw(10) << std::setfill('0') << size;
  return std::make_shared<const std::string>(ss.str() + uuidGenerateString());
}

std::size_t utility::uuidDecodeSize(const std::shared_ptr<const std::string>& uuid)
{
  /* valid sizes are 10 bytes for encoded size plus either 16 byte uuid binary or 36 byte uuid string representation */
  if (uuid && (uuid->size() == 46 || uuid->size() == 26)) {
    std::string size(uuid->substr(0, 10));
    return utility::Convert::toInt(size);
  }
  throw std::invalid_argument("invalid version supplied.");
}

std::shared_ptr<const std::string> utility::makeDataKey(const std::string& clusterId, const std::string& base,
                                                        int block_number)
{
  std::ostringstream ss;
  ss << clusterId << ":data:" << base << "_" << std::setw(10) << std::setfill('0') << block_number;
  return std::make_shared<const std::string>(ss.str());
}

std::shared_ptr<const std::string> utility::makeMetadataKey(const std::string& clusterId, const std::string& base)
{
  return std::make_shared<const std::string>(clusterId + ":metadata:" + base);
}

std::shared_ptr<const std::string> utility::makeAttributeKey(const std::string& clusterId, const std::string& path,
                                                             const std::string& attribute_name)
{
  return std::make_shared<const std::string>(clusterId + ":attribute:" + path + ":" + attribute_name);
}

std::string utility::extractAttributeName(const std::string& clusterId, const std::string& path,
                                          const std::string& attrkey)
{
  auto start = clusterId.size() + strlen(":attribute:") + path.size() + 1;
  return attrkey.substr(start);
}

std::shared_ptr<const std::string> utility::makeIndicatorKey(const std::string& key)
{
  return std::make_shared<const std::string>("indicator:" + key);
}

std::shared_ptr<const std::string> utility::indicatorToKey(const std::string& indicator_key)
{
  return std::make_shared<const std::string>(indicator_key.substr(strlen("indicator:"), std::string::npos));
}

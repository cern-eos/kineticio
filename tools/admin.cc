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

#include <kio/KineticIoFactory.hh>
#include <sys/syslog.h>
#include <iostream>
#include <unistd.h>
#include <signal.h>
#include <stdexcept>

namespace {

typedef kio::AdminClusterInterface::OperationTarget OperationTarget;

enum class Operation {
  STATUS, COUNT, SCAN, REPAIR, RESET, INVALID, CONFIG_SHOW, CONFIG_PUBLISH
};

struct Configuration {
  Operation op;
  std::vector<OperationTarget> targets;
  std::string id;
  std::string space;
  int numthreads;
  int verbosity;
  bool monitoring;
};

std::string to_str(OperationTarget target)
{
  switch (target) {
    case OperationTarget::ATTRIBUTE:
      return ("ATTRIBUTE");
    case OperationTarget::INVALID:
      return ("INVALID");
    case OperationTarget::INDICATOR:
      return ("INDICATOR");
    case OperationTarget::METADATA:
      return ("METADATA");
    case OperationTarget::DATA:
      return ("DATA");
  }
  return "INVALID";
}

int kinetic_help()
{
  fprintf(stdout, "------------------------------------------------------------------------------------------------\n");
#ifdef EOS
  fprintf(stdout, "usage: kinetic config [--publish] [--space <name>]\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       kinetic config [--space <name> ]\n");
  fprintf(stdout, "             shows the currently deployed kinetic configuration - by default 'default' space\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       kinetic config --publish [--space <name>]\n");
  fprintf(stdout, "             publishes the configuration files under <mgm>:/var/eos/kinetic/ to all currently\n");
  fprintf(stdout, "             existing FSTs in default or referenced space\n");
  fprintf(stdout, "\n");
#endif
  fprintf(stdout, "usage: kinetic --id <name> <operation> [OPTIONS] \n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       --id <name> \n");
  fprintf(stdout, "           the name of target cluster (see kinetic config)\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       <operation> \n");
  fprintf(stdout, "           status : show health status of cluster\n");
  fprintf(stdout, "           count  : count number of keys existing in the cluster\n");
  fprintf(stdout, "           scan   : check keys and display their status information\n");
  fprintf(stdout, "           repair : check keys, repair as required, display key status information\n");
  fprintf(stdout, "           reset  : force remove keys (Warning: Data will be lost!)\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "    OPTIONS\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       --target data|metadata|attribute|indicator  \n");
  fprintf(stdout, "           Operations can be limited to a specific key-type. Setting the 'indicator' type will \n");
  fprintf(stdout, "           perform the operation on keys of any type that have been marked as requiring repair.\n");
  fprintf(stdout, "           In most cases this is sufficient and much faster. Use full scan / repair operations \n");
  fprintf(stdout, "           after a drive replacement or cluster-wide power loss event. \n");
  fprintf(stdout, "\n");
#ifdef EOS
  fprintf(stdout, "       --space <name> \n");
  fprintf(stdout, "           Use the kinetic configuration for the referenced space - by default 'default' space \n");
  fprintf(stdout, "           is used (see kinetic config). \n");
  fprintf(stdout, "\n");
#else
  fprintf(stdout, "       --verbosity debug|notice|warning|error \n");
  fprintf(stdout, "           Specify verbosity level. Messages are printed to stdout (warning set as default). \n");
  fprintf(stdout, "\n");
#endif
  fprintf(stdout, "       --threads <number> \n");
  fprintf(stdout, "           Specify the number of background io threads used for the operation. \n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       -m : monitoring key=value output format\n");
  fprintf(stdout, "------------------------------------------------------------------------------------------------\n");
  return 0;
}

bool parseArguments(const std::vector<std::string>& arguments, Configuration& config)
{
  config.op = Operation::INVALID;
  config.numthreads = 1;
  config.verbosity = LOG_WARNING;
  config.monitoring = false;
  config.space = "default";

  for (unsigned i = 0; i < arguments.size(); i++) {
    if (arguments[i] == "scan") {
      config.op = Operation::SCAN;
    }
    else if (arguments[i] == "count") {
      config.op = Operation::COUNT;
    }
    else if (arguments[i] == "repair") {
      config.op = Operation::REPAIR;
    }
    else if (arguments[i] == "status") {
      config.op = Operation::STATUS;
    }
    else if (arguments[i] == "reset") {
      config.op = Operation::RESET;
    }
    else if (arguments[i] == "config") {
      config.op = Operation::CONFIG_SHOW;
      if (i + 1 < arguments.size() && arguments[i+1] == "--publish") {
        i++;
        config.op = Operation::CONFIG_PUBLISH;
      }
    }
    else if (arguments[i] == "-m") {
      config.monitoring = true;
    }
    else if (arguments.size() == i + 1) {
      /* all arguments beyond this point are pairs */
      return false;
    }
    else if (arguments[i] == "--target") {
      i++;
      if (arguments[i] == "indicator") {
        config.targets.push_back(OperationTarget::INDICATOR);
      }
      else if (arguments[i] == "data") {
        config.targets.push_back(OperationTarget::DATA);
      }
      else if (arguments[i] == "metadata") {
        config.targets.push_back(OperationTarget::METADATA);
      }
      else if (arguments[i] == "attribute") {
        config.targets.push_back(OperationTarget::ATTRIBUTE);
      }
      else {
        return false;
      }
    }
    else if (arguments[i] == "--verbosity") {
      i++;
      if (arguments[i] == "debug") {
        config.verbosity = LOG_DEBUG;
      }
      else if (arguments[i] == "notice") {
        config.verbosity = LOG_NOTICE;
      }
      else if (arguments[i] == "warning") {
        config.verbosity = LOG_WARNING;
      }
      else if (arguments[i] == "error") {
        config.verbosity = LOG_ERR;
      }
      else {
        return false;
      }
    }
    else if (arguments[i] == "--id") {
      config.id = std::string(arguments[++i]);
    }
    else if (arguments[i] == "--space") {
      config.space = std::string(arguments[++i]);
    }
    else if (arguments[i] == "--threads") {
      config.numthreads = atoi(arguments[++i].c_str());
    }
  }

  /* If no target(s) has/have been set, perform operation on all targets except indicator keys */
  if (config.targets.empty()) {
    config.targets = {OperationTarget::METADATA, OperationTarget::ATTRIBUTE, OperationTarget::DATA};
  }
  if (config.op == Operation::INVALID){
    return false;
  }
  if (config.id.empty() && !(config.op == Operation::CONFIG_PUBLISH || config.op == Operation::CONFIG_SHOW)){
    return false;
  }
  return true;
}


#ifdef EOS
XrdOucString
resultToString(XrdOucEnv* result)
{
  XrdOucString val = result->Get("mgm.proc.stdout");
  eos::common::StringTokenizer subtokenizer(val.c_str());

  if (subtokenizer.GetLine()) {
    XrdOucString nodeline = subtokenizer.GetToken();
    XrdOucString node = nodeline;
    node.erase(nodeline.find(":"));
    nodeline.erase(0, nodeline.find(":="));
    nodeline.erase(0, 2);

    // base 64 decode
    eos::common::SymKey::DeBase64(nodeline, val);
  }
  return val;
}

void
setEnvironmentVariables(Configuration& config)
{
  XrdOucString spacename = config.space.c_str();
  XrdOucString base = "mgm.cmd=space&mgm.subcmd=node-get&mgm.space=" + spacename + "&mgm.space.node-get.key=";

  XrdOucString location = base + "kinetic.location." + spacename;
  XrdOucString security = base + "kinetic.security." + spacename;
  XrdOucString cluster = base + "kinetic.cluster." + spacename;

  XrdOucEnv* location_result = client_admin_command(location);
  XrdOucEnv* security_result = client_admin_command(security);
  XrdOucEnv* cluster_result = client_admin_command(cluster);

  setenv("KINETIC_DRIVE_LOCATION", resultToString(location_result).c_str(), 1);
  setenv("KINETIC_DRIVE_SECURITY", resultToString(security_result).c_str(), 1);
  setenv("KINETIC_CLUSTER_DEFINITION", resultToString(cluster_result).c_str(), 1);
}

void
doConfig(Configuration& config)
{
  if (config.op == Operation::CONFIG_SHOW) {
    XrdOucString cmd1 = "node-get ";
    cmd1 += config.space.c_str();
    cmd1 += " kinetic.cluster.";
    cmd1 += config.space.c_str();
    XrdOucString cmd2 = "node-get ";
    cmd2 += config.space.c_str();
    cmd2 += " kinetic.location.";
    cmd2 += config.space.c_str();
    XrdOucString cmd3 = "node-get ";
    cmd3 += config.space.c_str();
    cmd3 += " kinetic.security.";
    cmd3 += config.space.c_str();

    com_space((char*) cmd1.c_str());
    com_space((char*) cmd2.c_str());
    com_space((char*) cmd3.c_str());
  }
  if (config.op == Operation::CONFIG_PUBLISH) {
    XrdOucString cmd1 = "node-set ";
    XrdOucString cmd2 = "node-set ";
    XrdOucString cmd3 = "node-set ";
    XrdOucString cmd4 = "node-set ";

    cmd1 += config.space.c_str();
    cmd1 += " kinetic.cluster.";
    cmd1 += config.space.c_str();
    cmd1 += " file:/var/eos/kinetic/kinetic-cluster-";
    cmd1 += config.space.c_str();
    cmd1 += ".json";

    cmd2 += config.space.c_str();
    cmd2 += " kinetic.location.";
    cmd2 += config.space.c_str();
    cmd2 += " file:/var/eos/kinetic/kinetic-location-";
    cmd2 += config.space.c_str();
    cmd2 += ".json";

    cmd3 += config.space.c_str();
    cmd3 += " kinetic.security.";
    cmd3 += config.space.c_str();
    cmd3 += " file:/var/eos/kinetic/kinetic-security-";
    cmd3 += config.space.c_str();
    cmd3 += ".json";

    // to trigger the configuration reload
    cmd4 += config.space.c_str();
    cmd4 += " kinetic.reload ";
    cmd4 += config.space.c_str();

    com_space((char*) cmd1.c_str());
    com_space((char*) cmd2.c_str());
    com_space((char*) cmd3.c_str());
    com_space((char*) cmd4.c_str());
  }
  return;
}
#else

bool mshouldLog(const char* func, int level, int target_level)
{
  return level <= target_level;
}

void mlog(const char* func, const char* file, int line, int level, const char* msg)
{
  switch (level) {
    case LOG_DEBUG:
      fprintf(stdout, "DEBUG:");
      break;
    case LOG_NOTICE:
      fprintf(stdout, "NOTICE:");
      break;
    case LOG_WARNING:
      fprintf(stdout, "WARNING:");
      break;
    case LOG_ERR:
      fprintf(stdout, "ERROR:");
      break;
  }
  fprintf(stdout, " %s\n", msg);
}
#endif

static bool continue_execution = true;

bool callbackfunction(bool do_print, int value)
{
  if (do_print) {
    fprintf(stdout, "\t %d\r", value);
    fflush(stdout);
  }
  return continue_execution;
}

void sigint_handler(int s)
{
  fprintf(stdout, "Caught SIGINT, initializing clean shutdown...\n");
  continue_execution = false;
}

typedef kio::AdminClusterInterface::KeyCounts KeyCounts;

const KeyCounts operator+(const KeyCounts& lhs, const KeyCounts& rhs)
{
  KeyCounts kc;
  kc.incomplete = lhs.incomplete + rhs.incomplete;
  kc.need_action = lhs.need_action + rhs.need_action;
  kc.removed = lhs.removed + rhs.removed;
  kc.repaired = lhs.repaired + rhs.repaired;
  kc.total = lhs.total + rhs.total;
  kc.unrepairable = lhs.unrepairable + rhs.unrepairable;
  return kc;
}

void printResults(const Configuration& config, KeyCounts& kc)
{
  if (config.monitoring) {
    fprintf(stdout, "kinetic.stat.keys.n=%d kinetic.stat.drives.inaccessible.n=%d kinetic.stat.require.action.n=%d "
                "kinetic.stat.repaired.n=%d kinetic.stat.removed.n=%d Kinetic.stat.notrepairable.n=%d\n",
            kc.total, kc.incomplete, kc.need_action, kc.repaired, kc.removed, kc.unrepairable);
    return;
  }

  fprintf(stdout, "\n");
  fprintf(stdout, "# ------------------------------------------------------------------------\n");
  fprintf(stdout, "# Total keys processed:                      %d \n", kc.total);
  if (config.op == Operation::SCAN) {
    fprintf(stdout, "# Keys where an action may be taken:         %d\n", kc.need_action);
    fprintf(stdout, "# Keys that are currently not readable:      %d\n", kc.unrepairable);
  }
  else if (config.op == Operation::REPAIR) {
    fprintf(stdout, "# Keys Repaired:                             %d\n", kc.repaired);
    fprintf(stdout, "# Orphaned chunks removed for:               %d\n", kc.removed);
    fprintf(stdout, "# Failed to repair:                          %d\n", kc.unrepairable);
  }
  else if (config.op == Operation::RESET) {
    fprintf(stdout, "# Keys removed:                              %d\n", kc.removed);
    fprintf(stdout, "# Failed to remove:                          %d\n", kc.unrepairable);
  }
  fprintf(stdout, "# Keys with chunks on inaccessible drives:   %d\n", kc.incomplete);
  fprintf(stdout, "# ------------------------------------------------------------------------\n");
}

int do_operation(Configuration& config)
{
  try {
    auto ac = kio::KineticIoFactory::makeAdminCluster(config.id.c_str());

    if (config.op == Operation::STATUS) {
      auto v = ac->status();
      if (config.monitoring) {
        fprintf(stdout, "kinetic.connections.total=%u kinetic.connections.failed=%u\n", v.drives_total,
                v.drives_failed);
        fprintf(stdout, "kinetic.redundancy_factor=%u\n", v.redundancy_factor);
        fprintf(stdout, "kinetic.indicator_exist=%s\n", v.indicator_exist ? "YES" : "NO");
        for (unsigned int i = 0; i < v.connected.size(); i++) {
          fprintf(stdout, "kinetic.drive.index=%u kinetic.drive.status=%s\n", i, v.connected[i] ? "OK" : "FAILED");
        }
      }
      else {
        fprintf(stdout, "# ------------------------------------------------------------------------\n");
        fprintf(stdout, "# Cluster Status\n");
        fprintf(stdout, "# \tConnections Failed: %u of %u \n", v.drives_failed, v.drives_total);
        fprintf(stdout, "# \tRedundancy Factor: %u\n", v.redundancy_factor);
        fprintf(stdout, "# \tIndicator keys: %s \n", v.indicator_exist ? "EXIST" : "NONE");
        fprintf(stdout, "# ------------------------------------------------------------------------\n");
        for (unsigned int i = 0; i < v.connected.size(); i++) {
          fprintf(stdout, "# drive %2u : %s %s\n", i, v.connected[i] ? "OK" : "FAILED", v.location[i].c_str());
        }
      }
    }
    else {
      /* Register SIGINT to enable clean shutdown if client decides to control+c */
      struct sigaction sigIntHandler;
      sigIntHandler.sa_handler = sigint_handler;
      sigemptyset(&sigIntHandler.sa_mask);
      sigIntHandler.sa_flags = 0;
      sigaction(SIGINT, &sigIntHandler, NULL);
      continue_execution = true;
      auto callback = std::bind(callbackfunction, !config.monitoring, std::placeholders::_1);

      int tcount = 0;
      KeyCounts tstats{0, 0, 0, 0, 0, 0};

      for (unsigned i = 0; i < config.targets.size(); i++) {

        auto target = config.targets[i];
        if (!config.monitoring) {
          printf("Performing operation on all %s keys of the cluster... \n", to_str(target).c_str());
        }
        switch (config.op) {
          case Operation::COUNT:
            tcount += ac->count(target, callback);
            break;
          case Operation::SCAN:
            tstats = tstats + ac->scan(target, callback, config.numthreads);
            break;
          case Operation::REPAIR:
            tstats = tstats + ac->repair(target, callback, config.numthreads);
            break;
          case Operation::RESET:
            tstats = tstats + ac->reset(target, callback, config.numthreads);
            break;
          default:
            throw std::runtime_error("No valid operation specified.");
        }
        if (!config.monitoring) {
          fprintf(stdout, "\n");
        }
      }

      if (config.op == Operation::COUNT) {
        if (config.monitoring) {
          fprintf(stdout, "kinetic.stat.keys.n=%d\n", tcount);
        }
        else {
          fprintf(stdout, "\n");
          fprintf(stdout, "# ------------------------------------------------------------------------\n");
          fprintf(stdout, "# Completed Operation - Counted a total of %d keys\n", tcount);
          fprintf(stdout, "# ------------------------------------------------------------------------\n");
        }
      }
      else {
        printResults(config, tstats);
      }
    }
  } catch (std::exception& e) {
    fprintf(stdout, "Encountered Exception: %s\n", e.what());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
}

#ifdef EOS
int
com_kinetic(char* arg)
{
  std::vector<std::string> arguments;
  eos::common::StringTokenizer subtokenizer(arg);
  subtokenizer.GetLine();
  XrdOucString str = subtokenizer.GetToken();
  while (str.length()) {
    arguments.push_back(str.c_str());
    str = subtokenizer.GetToken();
  }
  Configuration config;
  if (!parseArguments(arguments, config)) {
    kinetic_help();
    return EXIT_FAILURE;
  }
  if (config.op == Operation::CONFIG_SHOW || config.op == Operation::CONFIG_PUBLISH) {
    doConfig(config);
    return EXIT_SUCCESS;
  }
  setEnvironmentVariables(config);
  return do_operation(config);
}
#else

int main(int argc, char** argv)
{
  std::vector<std::string> arguments;
  for (int i = 1; i < argc; i++) {
    arguments.push_back(argv[i]);
  }
  Configuration config;
  if (!parseArguments(arguments, config)) {
    kinetic_help();
    return EXIT_FAILURE;
  }

  kio::KineticIoFactory::registerLogFunction(
      mlog, std::bind(mshouldLog, std::placeholders::_1, std::placeholders::_2, config.verbosity)
  );
  return do_operation(config);
}

#endif
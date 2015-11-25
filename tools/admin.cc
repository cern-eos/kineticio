#include <iostream>
#include <sys/syslog.h>
#include <unistd.h>
#include "AdminClusterInterface.hh"
#include "ClusterMap.hh"
#include "KineticIoFactory.hh"

typedef kio::AdminClusterInterface::OperationTarget OperationTarget;

enum class Operation{
  STATUS, COUNT, SCAN, REPAIR, RESET, INVALID
};

struct Configuration{
    Operation op;
    OperationTarget target; 
    std::string id;
    int numthreads;
    int verbosity;
    bool monitoring;
};

int kinetic_help(){
//  fprintf(stdout, "usage: kinetic config [--publish] [--space <space> ]\n");
//  fprintf(stdout, "       kinetic config [--space <space> ]                     : shows the currently deployed kinetic configuration - by default 'default' space\n");
//  fprintf(stdout, "       kinetic config --publish                              : publishes the default MGM configuration files under <mgm>:/var/eos/kinetic to all currently existing FSTs in default or referenced space\n");
//  fprintf(stdout, "       kinetic config --publish [--space <name>]             : identifier <name> refers to the MGM files like /var/eos/kinetic/kinetic-cluster-<name>.json /var/eos/kinetic/kinetic-drives-<name>.json /var/eos/kinetic/kinetic-security-<name>.json\n");
//  fprintf(stdout, "\n");
  fprintf(stdout, "usage: kinetic --id <clusterid> <operation> <target> [--threads <numthreads>] [--verbosity debug|notice|warning|error] [-m]\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       kinetic ... --id <clusterid> ...                      : specify cluster, <clusterid> refers to the name of the cluster set in the cluster configuration\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       kinetic ... <operation> <target> ...                  : run <operation> on keys of type <target>\n");
  fprintf(stdout, "         <operation>\n");
  fprintf(stdout, "             status                                          : show connection status, does not require a <target> type\n");
  fprintf(stdout, "             count                                           : count number of keys existing in the cluster\n");
  fprintf(stdout, "             scan                                            : check keys and display their status information (Warning: Long Runtime)\n");
  fprintf(stdout, "             repair                                          : check keys, repair as required, display key status information (Warning: Long Runtime)\n");
  fprintf(stdout, "             reset                                           : force remove keys (Warning: Data will be lost!)\n");
  fprintf(stdout, "         <target>\n");
  fprintf(stdout, "             data                                            : data keys\n");
  fprintf(stdout, "             metadata                                        : metadata keys\n");
  fprintf(stdout, "             attribute                                       : attribute keys\n");
  fprintf(stdout, "             indicator                                       : keys with indicators (written automatically when encountering partial failures during normal operation)\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       kinetic ... [--threads <numthreads>] ...              : (optional) specify the number of background io threads \n");
  fprintf(stdout, "       kinetic ... [--verbosity debug|notice|warning|error]  : (optional) specify verbosity level, warning is set as default \n");
  fprintf(stdout, "       kinetic ... [-m] ...                                  : (optional) monitoring key=value output format\n");  

  return 0;
}

void printKeyCount(const kio::AdminClusterInterface::KeyCounts& kc)
{
  fprintf(stdout, "\n");
  fprintf(stdout, "# ------------------------------------------------------------------------\n");
  fprintf(stdout, "# Completed Operation - scanned a total of %d keys\n", kc.total);
  fprintf(stdout, "# ------------------------------------------------------------------------\n");
  fprintf(stdout, "# Keys with inaccessible drives:        %d\n", kc.incomplete);
  fprintf(stdout, "# Keys requiring action:                %d\n", kc.need_action);
  fprintf(stdout, "# Keys Repaired:                        %d\n", kc.repaired);
  fprintf(stdout, "# Keys Removed:                         %d\n", kc.removed);
  fprintf(stdout, "# Not repairable:                       %d\n", kc.unrepairable);
  fprintf(stdout, "# ------------------------------------------------------------------------\n");
}

bool mshouldLog (const char* func, int level, int target_level)
{
  return level <= target_level;
}

void mlog(const char* func, const char* file, int line, int level, const char* msg){
  switch(level){
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

bool parseArguments(int argc, char** argv, Configuration& config) {
  config.op = Operation::INVALID;
  config.target = OperationTarget::INVALID;
  config.numthreads = 1;
  config.verbosity = LOG_WARNING;
  config.monitoring = false;

  for(int i = 1; i < argc; i++){
    if(strcmp("--id", argv[i]) == 0)
      config.id = std::string(argv[i+1]);
    else if(strcmp("scan", argv[i]) == 0)
      config.op = Operation::SCAN;
    else if(strcmp("count", argv[i]) == 0)
      config.op = Operation::COUNT;
    else if(strcmp("repair", argv[i]) == 0)
      config.op = Operation::REPAIR;
    else if(strcmp("status", argv[i]) == 0)
      config.op = Operation::STATUS;
    else if(strcmp("reset", argv[i]) == 0)
      config.op = Operation::RESET;
    else if(strcmp("indicator", argv[i]) == 0)
      config.target = OperationTarget::INDICATOR;
    else if(strcmp("data", argv[i]) == 0)
      config.target = OperationTarget::DATA;
    else if(strcmp("metadata", argv[i]) == 0)
      config.target = OperationTarget::METADATA;
    else if(strcmp("attribute", argv[i]) == 0)
      config.target = OperationTarget::ATTRIBUTE;
    else if(strcmp("-m", argv[i]) == 0)
      config.monitoring = true;
    else if(strcmp("--verbosity", argv[i]) == 0){
        if(strcmp("debug", argv[i+1]) == 0)
          config.verbosity = LOG_DEBUG;
        else if(strcmp("notice", argv[i+1]) == 0)
          config.verbosity = LOG_NOTICE;
        else if(strcmp("warning", argv[i+1]) == 0)
          config.verbosity = LOG_WARNING;
        else if(strcmp("error", argv[i+1]) == 0)
          config.verbosity = LOG_ERR;   
    }
    else if(strcmp("--threads", argv[i]) == 0)
      config.numthreads = atoi(argv[i+1]);
  }
  
  if(!config.id.length())
      return false;
  if(config.op == Operation::INVALID)
      return false;
  if(config.op != Operation::STATUS &&  config.target == OperationTarget::INVALID)
      return false;
  return true;
}

void printinc(int value){
  fprintf(stdout, "\r\t %d",value);
  fflush(stdout);
}

int main(int argc, char** argv)
{
  Configuration config;
  if(!parseArguments(argc, argv, config)){
    fprintf(stdout, "Incorrect arguments\n");
    kinetic_help();
    return EXIT_FAILURE;
  }

  try{
    kio::Factory::registerLogFunction(mlog,
                                      std::bind(mshouldLog, std::placeholders::_1, std::placeholders::_2, config.verbosity)
                                      );
    auto ac = kio::Factory::makeAdminCluster(config.id.c_str());

    switch(config.op){
      case Operation::STATUS: {
        if (!config.monitoring) {
          fprintf(stdout, "# ------------------------------------------------------------------------\n");
          fprintf(stdout, "# Cluster Status: \n");
          fprintf(stdout, "# ------------------------------------------------------------------------\n");
        }
        auto v = ac->status();
        for (size_t i = 0; i < v.size(); i++) {
          if (config.monitoring)
            fprintf(stdout, "kinetic.drive.index=%lu kinetic.drive.status=%s\n", i, v[i] ? "OK" : "FAILED");
          else
            fprintf(stdout, "# drive %5d : %s\n", (int)i, v[i] ? "OK" : "FAILED");
        }
        break;
      }
      case Operation::COUNT:
        fprintf(stdout, "Counting number of keys on cluster: \n");
        ac->count(config.target, printinc);
        fprintf(stdout, "\n");
        break;
      case Operation::SCAN:
        fprintf(stdout, "Scanning keys on cluster: \n");
        printKeyCount( ac->scan(config.target, printinc, config.numthreads) );
        break;
      case Operation::REPAIR:
        fprintf(stdout, "Scan & repair of keys on cluster: \n");
        printKeyCount( ac->repair(config.target, printinc, config.numthreads) );
        break;
        case Operation::RESET:
        fprintf(stdout, "Removing keys from cluster: \n");
        printKeyCount( ac->reset(config.target, printinc, config.numthreads) );
        break;
    }
  }catch(std::exception& e){
    fprintf(stdout, "Encountered Exception: %s\n", e.what());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
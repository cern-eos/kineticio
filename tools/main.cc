#include <iostream>
#include <sys/syslog.h>
#include <unistd.h>
#include "ClusterMap.hh"
#include "KineticIoFactory.hh"

using std::cout;
using std::endl;

enum class Operation{
    STATUS, COUNT, SCAN, REPAIR, RESET, INVALID
};

struct Configuration{
    Operation op;
    std::string id;
};

int kinetic_help(){
  fprintf(stdout, "'[eos] kinetic ..' provides the kinetic cluster management interface of EOS.\n");
  fprintf(stdout, " Usage: kinetic -cluster id status|count|scan|repair|reset \n");
  fprintf(stdout, "    status: print status of connections of the cluster. \n");
  fprintf(stdout, "    count: number of keys existing in the cluster. \n");
  fprintf(stdout, "    scan: check all keys existing in the cluster and display their status information (Warning: Long Runtime) \n");
  fprintf(stdout, "    repair: check all keys existing in the cluster, repair as required, display their status information. (Warning: Long Runtime) \n");
  fprintf(stdout, "    reset: force remove all keys on all drives associated with the cluster, you will loose ALL data! \n");
  fprintf(stdout, " Usage: kinetic -config reload \n");
  fprintf(stdout, "    reload: reload the json config files, existing transfers continue using previous cluster configuration. \n");
  return 0;
}

void printKeyCount(const kio::AdminClusterInterface::KeyCounts& kc)
{
  fprintf(stdout, "Completed Operation. Scanned a total of %d keys\n", kc.total);
  fprintf(stdout, "Stripes with inaccessible drives: %d\n", kc.incomplete);
  fprintf(stdout, "Can Repair / Remove: %d\n", kc.need_repair);
  fprintf(stdout, "Repaired: %d\n", kc.repaired);
  fprintf(stdout, "Removed: %d\n", kc.removed);
  fprintf(stdout, "Unrepairable: %d\n", kc.unrepairable);
}

bool mshouldLog(const char* func, int level){
  if(level < LOG_DEBUG)
    return true;
  return false;
}

void mlog(const char* func, const char* file, int line, int level, const char* msg){
  switch(level){
    case LOG_DEBUG:
      cout << "DEBUG:";
      break;
    case LOG_NOTICE:
      cout << "NOTICE:";
      break;
    case LOG_WARNING:
      cout << "WARNING:";
      break;
  }
  std::cout << " " << msg << endl;
}


bool parseArguments(int argc, char** argv, Configuration& config) {
  if(argc!=4)
    return false;
  config.op = Operation::INVALID;

  for(int i = 1; i < argc; i++){
    if(strcmp("-id", argv[i]) == 0)
      config.id = std::string(argv[i+1]);
    else if(strcmp("-scan", argv[i]) == 0)
      config.op = Operation::SCAN;
    else if(strcmp("-count", argv[i]) == 0)
      config.op = Operation::COUNT;
    else if(strcmp("-repair", argv[i]) == 0)
      config.op = Operation::REPAIR;
    else if(strcmp("-status", argv[i]) == 0)
      config.op = Operation::STATUS;
    else if(strcmp("-reset", argv[i]) == 0)
      config.op = Operation::RESET;
  }

  return config.id.length() && config.op != Operation::INVALID;
}

int main(int argc, char** argv)
{
  Configuration config;
  if(!parseArguments(argc, argv, config)){
    cout << "Incorrect arguments" << endl;
    kinetic_help();
    return EXIT_FAILURE;
  }

  try{
    kio::Factory::registerLogFunction(mlog, mshouldLog);
    auto ac = kio::Factory::makeAdminCluster(config.id.c_str());

    switch(config.op){
      case Operation::STATUS: {
        sleep(1);
        auto v = ac->status();
        cout << "Cluster Status: ";
        for(auto it=v.cbegin(); it!=v.cend(); it++)
          cout << *it;
        cout << endl;
        break;
      }
      case Operation::COUNT: {
        cout << "Total number of keys on cluster: " << ac->count() << endl;
        break;
      }
      case Operation::SCAN: {
        printKeyCount(ac->scan());
        break;
      }
      case Operation::REPAIR: {
        printKeyCount(ac->repair());
        break;
      }
      case Operation::RESET: {
        printKeyCount(ac->reset());
        break;
      }
    }
  }catch(std::exception& e){
    cout << "Encountered Exception: " << e.what() << endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
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
    int numthreads;
};

int kinetic_help(){
  fprintf(stdout, "'kinetic ..' provides the kinetic cluster management interface of EOS.\n");
  fprintf(stdout, " Usage: kinetic -id clusterid status|count|scan|repair|reset [-threads numthreads]\n");
  fprintf(stdout, "    clusterid: the unique name of the cluster. \n");
  fprintf(stdout, "    numthreads: number of io threads (optional). \n");
  fprintf(stdout, "    status: print status of connections of the cluster. \n");
  fprintf(stdout, "    count: number of keys existing in the cluster. \n");
  fprintf(stdout, "    scan: check all keys existing in the cluster and display their status information (Warning: Long Runtime) \n");
  fprintf(stdout, "    repair: check all keys existing in the cluster, repair as required, display their status information. (Warning: Long Runtime) \n");
  fprintf(stdout, "    reset: force remove all keys on all drives associated with the cluster, you will loose ALL data! \n");
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
  config.op = Operation::INVALID;
  config.numthreads = 1;

  for(int i = 1; i < argc; i++){
    if(strcmp("-id", argv[i]) == 0)
      config.id = std::string(argv[i+1]);
    if(strcmp("-threads", argv[i]) == 0)
      config.numthreads = atoi(argv[i+1]);
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

kio::AdminClusterInterface::KeyCounts operator+= (kio::AdminClusterInterface::KeyCounts& lhs, const kio::AdminClusterInterface::KeyCounts& rhs)
{
  lhs.total+=rhs.total;
  lhs.incomplete+=rhs.incomplete;
  lhs.need_repair+=rhs.need_repair;
  lhs.removed+=rhs.removed;
  lhs.repaired+=rhs.repaired;
  lhs.unrepairable+=rhs.unrepairable;
  return lhs;
}

int countkeys(std::unique_ptr<kio::AdminClusterInterface>& ac){
  fprintf(stdout, "Counting number of keys on cluster: \n");
  auto total = 0;
  while(true){
    auto c = ac->count(5000);
    if(!c) break;
    total += c;
    fprintf(stdout, "\r\t %d",total);
    fflush(stdout);
  }
  fprintf(stdout, "\n");
  return total;
}

kio::AdminClusterInterface::KeyCounts doOperation(
    std::unique_ptr<kio::AdminClusterInterface>& ac,
    Configuration& config
)
{

  kio::AdminClusterInterface::KeyCounts kc{0,0,0,0,0,0};
  auto totalkeys = countkeys(ac);
  auto numsteps = 50;
  auto perstep = (totalkeys+numsteps-1) / numsteps;

  for(int i=0; i<numsteps; i++){
    switch(config.op){
      case Operation::SCAN:
        kc += ac->scan(perstep, i==0);
        break;
      case Operation::REPAIR:
        kc += ac->repair(perstep, i==0);
        break;
      case Operation::RESET:
        kc += ac->reset(perstep, i==0);
        break;
    }
    fprintf(stdout, "\r[");
    for(int j=0; j<=i; j++)
      fprintf(stdout, "*");
    for(int j=i+1; j<numsteps; j++)
      fprintf(stdout, "-");
    fprintf(stdout, "]");
    fflush(stdout);
  }
  fprintf(stdout, "\n");
  printKeyCount(kc);
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
    auto ac = kio::Factory::makeAdminCluster(config.id.c_str(), config.numthreads);

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
        countkeys(ac);
        break;
      }
      default:
        doOperation(ac, config);
    }
  }catch(std::exception& e){
    cout << "Encountered Exception: " << e.what() << endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
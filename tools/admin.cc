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
};

int kinetic_help(){
  fprintf(stdout, " Usage: -id clusterid -op status|count|scan|repair|reset -target attribute|file|indicator|all [-threads numthreads]\n"); 
  fprintf(stdout, " -id: specify the cluster identifier \n");
  fprintf(stdout, " -op: specify one of the following operations to execute\n");
  fprintf(stdout, "    status: print status of connections of the cluster. \n");
  fprintf(stdout, "    count: number of keys existing in the cluster. \n");
  fprintf(stdout, "    scan: check all keys existing in the cluster and display their status information (Warning: Long Runtime) \n");
  fprintf(stdout, "    repair: check all keys existing in the cluster, repair as required, display their status information. (Warning: Long Runtime) \n");
  fprintf(stdout, "    reset: force remove all keys on all drives associated with the cluster, you will loose ALL data! \n");
  fprintf(stdout, " -target: specify one of the following target ranges, required for any operation except status \n");
  fprintf(stdout, "    all: perform operation on all keys of the cluster\n");
  fprintf(stdout, "    file: perform operation on keys associated with files\n");
  fprintf(stdout, "    attribute: perform operation on attribute keys only \n");
  fprintf(stdout, "    indicator: perform operation only on keys with indicators (written automatically when encountering partial failures during a get/put/remove in normal operation)\n");
  fprintf(stdout, " -threads: (optional) specify the number of background io threads \n");
  return 0;
}

void printKeyCount(const kio::AdminClusterInterface::KeyCounts& kc)
{
  fprintf(stdout, "Completed Operation. Scanned a total of %d keys\n\n", kc.total);
  fprintf(stdout, "Keys with inaccessible drives: %d\n", kc.incomplete);
  fprintf(stdout, "Keys requiring action: %d\n", kc.need_action);
  fprintf(stdout, "Keys Repaired: %d\n", kc.repaired);
  fprintf(stdout, "Keys Removed: %d\n", kc.removed);
  fprintf(stdout, "Not repairable: %d\n", kc.unrepairable);
}

bool mshouldLog(const char* func, int level){
  if(level < LOG_DEBUG)
    return true;
  return false;
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
  }
  fprintf(stdout, " %s\n", msg);
}


bool parseArguments(int argc, char** argv, Configuration& config) {
  config.op = Operation::INVALID;
  config.target = OperationTarget::INVALID;
  config.numthreads = 1;

  for(int i = 1; i < argc; i++){
    if(strcmp("-id", argv[i]) == 0)
      config.id = std::string(argv[i+1]);
    else if(strcmp("-threads", argv[i]) == 0)
      config.numthreads = atoi(argv[i+1]);
    else if(strcmp("-op",argv[i]) == 0){
      if(strcmp("scan", argv[i+1]) == 0)
        config.op = Operation::SCAN;
      else if(strcmp("count", argv[i+1]) == 0)
        config.op = Operation::COUNT;
      else if(strcmp("repair", argv[i+1]) == 0)
        config.op = Operation::REPAIR;
      else if(strcmp("status", argv[i+1]) == 0)
        config.op = Operation::STATUS;
      else if(strcmp("reset", argv[i+1]) == 0)
        config.op = Operation::RESET;
    }
    else if(strcmp("-target",argv[i]) == 0){
      if(strcmp("all", argv[i+1]) == 0)
        config.target = OperationTarget::ALL;
      else if(strcmp("indicator", argv[i+1]) == 0)
        config.target = OperationTarget::INDICATOR;
      else if(strcmp("file", argv[i+1]) == 0)
        config.target = OperationTarget::FILE;
      else if(strcmp("attribute", argv[i+1]) == 0)
        config.target = OperationTarget::ATTRIBUTE;
     }
  }

  return config.id.length() && config.op != Operation::INVALID && config.target != OperationTarget::INVALID;
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
  fprintf(stdout, "\r\t %d\n",total);;
  return total;
}

kio::AdminClusterInterface::KeyCounts doOperation(
    std::unique_ptr<kio::AdminClusterInterface>& ac,
    Configuration& config
)
{
  const auto totalkeys = countkeys(ac);
  auto numsteps = 50;
  const auto perstep = (totalkeys+numsteps-1) / numsteps;

  int step = perstep; 
  
  for(int i=0; step; i++){
    switch(config.op){
      case Operation::SCAN:
        step = ac->scan(perstep, i==0);
        break;
      case Operation::REPAIR:
        step = ac->repair(perstep, i==0);
        break;
      case Operation::RESET:
        step = ac->reset(perstep, i==0);
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
  printKeyCount(ac->getCounts());
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
    kio::Factory::registerLogFunction(mlog, mshouldLog);
    auto ac = kio::Factory::makeAdminCluster(config.id.c_str(), config.target, config.numthreads);

    switch(config.op){
      case Operation::STATUS: {
        sleep(1);
        auto v = ac->status();
        fprintf(stdout, "Cluster Status: \n");
        for(int i=0; i<v.size(); i++) 
          fprintf(stdout, "drive %d: %s\n",i,v[i]?"OK":"FAILED");
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
    fprintf(stdout, "Encountered Exception: %s\n", e.what());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
#include <iostream>
#include <sys/syslog.h>
#include <unistd.h>
#include "ClusterMap.hh"
#include "KineticIoFactory.hh"

using std::cout;
using std::endl;

enum class Operation{
    STATUS, COUNT, SCAN, REPAIR, INVALID
};

struct Configuration{
    Operation op;
    std::string id;
};

void usage(){
  cout << "-------------------------------------------------------" << endl;
  cout << "-id clusterID, where clusterID is a valid cluster id   " << endl;
  cout << "-status, obtain the cluster status." << endl;
  cout << "-count, count number of keys on the cluster " << endl;
  cout << "-scan, perform a scan only, no repair will be attempted" << endl;
  cout << "-repair, scan & repair all keys possible in the cluster" << endl;
  cout << "-------------------------------------------------------" << endl;
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
  }

  return config.id.length() && config.op != Operation::INVALID;
}

void printKeyCount(const kio::KineticAdminCluster::KeyCounts& kc)
{
  cout << "Completed Operation. Scanned a total of " << kc.total << " keys." << endl;
  cout << "Incomplete: " << kc.incomplete << endl;
  cout << "Need Repair: " << kc.need_repair << endl;
  cout << "Repaired: " << kc.repaired << endl;
  cout << "Removed: " << kc.removed << endl;
  cout << "Unrepairable: " << kc.unrepairable << endl;
}

int main(int argc, char** argv)
{
  Configuration config;
  if(!parseArguments(argc, argv, config)){
    cout << "Incorrect arguments" << endl;
    usage();
    return EXIT_FAILURE;
  }

  try{
    kio::Factory::registerLogFunction(mlog, mshouldLog);
    auto ac = kio::ClusterMap::getInstance().getAdminCluster(config.id);

    switch(config.op){
      case Operation::STATUS: {
        ac->status();
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
    }
  }catch(std::exception& e){
    cout << "Encountered Exception: " << e.what() << endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
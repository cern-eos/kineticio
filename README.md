# kineticio
Providing a file interface (byterange read / write access) to kinetic hardware. While this library can be compiled on its own, its primary use case is as an EOS plugin. The exposed interface and behavior thus closely mirror eos::fst::FileIO. Due to supporting standard ScientificLinux 6 as a target platform, the use of c++11 in the source code is limited to the subset available in the gcc 4.4.7 compiler.

### Dependencies
Project build dependencies:
``` 
yum install cmake json-c json-c-devel openssl openssl-devel yasm gcc-c++ gcc protobuf protobuf-compiler protobuf-devel libuuid libuuid-devel 
```

The kinetic-cpp-client and the [isa-l](https://01.org/intel%C2%AE-storage-acceleration-library-open-source-version) erasure coding library will be compiled automatically if they are not found, as there are no standard packages available. 

Standard behavior is to link all dependencies dynamically. If desired, it is possible to link the kinetic-cpp-client statically so that it does not have to be available at all machines running the kineticio library. To enable static linking, run cmake with -DLINK_STATIC=ON 

To build the provided tests, run cmake with -DBUILD_TEST=ON option. There are additional dependencies for building the test system:

 + [Maven](https://maven.apache.org) is required to compile the kinetic simulator and not available in standard yum repos.  
 + ``` yum install java-1.7.0-openjdk zlib ```

Project run dependencies: 
``` json-c openssl libuuid protobuf isa-l (kinetic-cpp-client) ``` 

### Configuration
The **KINETIC\_DRIVE\_LOCATION**, **KINETIC\_DRIVE\_SECURITY** and **KINETIC\_CLUSTER\_DEFINITION** environment variables have to be set and specify drive location, login details and the cluster configuration (as well as library-wide configuration options) respectively. They may either contain the information themselves, or point to json file(s) that contain the respective information. See [localhost.json](test/localhost.json) as an example.  

### EOS support
When this library is installed, kinetic support will automatically be enabled when compiling the kinetic branch of the [EOS repository](https://gitlab.cern.ch/dss/eos/tree/kinetic).

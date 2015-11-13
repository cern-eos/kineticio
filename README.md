# kineticio
Providing a file interface (byterange read / write access) to kinetic hardware. While this library can be compiled on its own, its primary use case is as an EOS plugin. The exposed interface and behavior thus closely mirror eos::fst::FileIO. Due to supporting standard ScientificLinux 6 as a target platform, the use of c++11 in the source code is limited to the subset available in the gcc 4.4.7 compiler.

### Dependencies
 + ``` cmake git gcc gcc-c++ json-c json-c-devel libuuid libuuid-devel ```
 + the [isa-l](https://01.org/intel%C2%AE-storage-acceleration-library-open-source-version) erasure coding library and the [kinetic-cpp-client](https://github.com/kinetic/kinetic-cpp-client) (if you have trouble building / installing try [this](https://github.com/plensing/kinetic-cpp-client) fork)
 + to build the test system, run cmake with -DBUILD_TEST=ON option. Additional dependencies: [Maven](https://maven.apache.org), ```java-1.7.0-openjdk zlib```

### Configuration
The **KINETIC\_DRIVE\_LOCATION**, **KINETIC\_DRIVE\_SECURITY** and **KINETIC\_CLUSTER\_DEFINITION** environment variables have to be set and specify drive location, login details and the cluster configuration (as well as library-wide configuration options) respectively. The environment variables may either contain the information themselves, or point to one or multiple json files containing the respective information. See [localhost.json](test/localhost.json) as an example.  

### EOS support
When this library is installed, kinetic support will automatically be enabled when compiling the kinetic branch of the [EOS repository](https://gitlab.cern.ch/dss/eos/tree/kinetic).

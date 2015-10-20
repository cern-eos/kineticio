# kineticio
Providing a file interface (byterange read / write access) to kinetic hardware. While this library can be compiled on its own, its primary use case is as an EOS plugin. The exposed interface and behavior thus closely mirror eos::fst::FileIO. Due to supporting standard ScientificLinux 6 as a target platform, the use of c++11 in the source code is limited to the subset available in the gcc 4.4.7 compiler.

### Dependencies
There are two ways to build the kineticio library. 

#### Linking all dependencies dynamically 
+ [CMake](http://www.cmake.org) build process manager
+ [json-c](https://github.com/json-c/json-c) JSON manipulation library
+ [isa-l](https://01.org/intel%C2%AE-storage-acceleration-library-open-source-version) for erasure coding
+ **kinetic-cpp-client** c++ library implementation for the kinetic protocol. [This](https://github.com/plensing/kinetic-cpp-client) fork builds a shared library linking preinstalled dependencies instead of making dependencies during the build process. This can prevent conflicts when linking the library against a project (e.g. EOS) that has some of the same dependencies.  

#### Linking non-standard dependencies statically
For a more straightforward installation, the kinetic-cpp-client and isa-l libraries (and most of their dependencies) can be built and linked statically.  
+ You will need the following packages: yasm openssl openssl-devel protobuf protobuf-compiler
+ Call Cmake with -DLINK_STATIC=ON 
 
### Configuration
The **KINETIC_DRIVE_LOCATION**, **KINETIC_DRIVE_SECURITY** and **KINETIC_CLUSTER_DEFINITION** environment variables have to be set and specify drive location, login details and the cluster configuration (as well as library-wide configuration options) respectively. They may either contain the information themselves, or point to json file(s) that contain the respective information. See [localhost.json](test/localhost.json) as an example.  

### EOS support
When this library is installed, kinetic support will automatically be enabled when compiling the kinetic branch of the [EOS repository](https://gitlab.cern.ch/dss/eos/tree/kinetic).

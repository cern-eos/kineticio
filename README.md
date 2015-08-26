# kineticio
Providing a file interface (byterange read / write access) to kinetic hardware. While this library can be compiled on its own, its primary use case is as an EOS plugin. The exposed interface and behavior thus closely mirror eos::fst::FileIO. Due to supporting standard ScientificLinux 6 as a target platform, the use of c++11 in the source code is limited to the subset available in the gcc 4.4.7 compiler.

### Dependencies
+ [CMake](http://www.cmake.org) build process manager
+ [json-c](https://github.com/json-c/json-c) JSON manipulation library
+ [zlib](http://www.zlib.net/) for checksum calculation
+ [isa-l](https://01.org/intel%C2%AE-storage-acceleration-library-open-source-version) for erasure coding
+ **kinetic-cpp-client** c++ library implementation for the kinetic protocol. [This](https://github.com/plensing/kinetic-cpp-client) fork builds a shared library linking preinstalled dependencies instead of making dependencies during the build process. This can prevent conflicts when linking the library against a project (e.g. EOS) that has some of the same dependencies.  

### Initial Setup
+ Install any missing dependencies (use yum / apt-get where possible)
+ Clone the git repository
+ Create a build directory. If you want you can use the cloned git repository as your build directory, but using a separate directory is recommended in order to cleanly separate sources and generated files. 
+ From your build directory call `cmake /path/to/cloned-git`, if you're using the cloned git repository as your build directory this would be `cmake .`
+ Run `make` && `make install`

### Configuration
The **KINETIC_DRIVE_LOCATION**, **KINETIC_DRIVE_SECURITY** and **KINETIC_CLUSTER_DEFINITION** environment variables have to be set and point to json file(s) listing drive location, login details and the cluster configuration respectively. This can, but does not have to, be one and the same file. The json file at **KINETIC_CLUSTER_DEFINITION** may further include library-wide configuration options. See [localhost.json](test/localhost.json) as an example. 

### EOS support
When this library is installed, kinetic support will automatically be enabled when compiling the kinetic branch of the [EOS repository](http://eos.cern.ch/cgi-bin/cgit.cgi/eos/).
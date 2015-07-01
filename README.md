# kineticio
Providing a file interface (byterange read / write access) to kinetic hardware. While this library can be compiled on its own, its primary use case is as an EOS plugin. The exposed interface and behavior thus closely mirror eos::fst::FileIO. 

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
The **KINETIC_DRIVE_LOCATION** and **KINETIC_DRIVE_SECURITY** environment variables have to be set and point to json files listing drive location and security inforamtion respectively. This can, but does not have to, be one and the same file. See [localhost.json](test/localhost.json) as an example. 

### EOS support
When this library is installed, kinetic support will automatically be enabled when compiling the kinetic branch of the official EOS repository. 
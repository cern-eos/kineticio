# kineticio [![Build Status](https://travis-ci.org/cern-eos/kineticio.svg?branch=master)](https://travis-ci.org/cern-eos/kineticio)

Kineticio is a library providing a file interface for the [Kinetic](https://www.openkinetic.org/) storage technology. 

It allows applications and storage systems relying on a standard file interface (or even just byterange read / write access) to easily integrate kinetic devices as a storage backend. 

It does not, on its own, provide full file system functionality. 

The primary use case of this library is to enable Kinetic capability for the [EOS](https://gitlab.cern.ch/dss/eos/) storage system. The exposed interface and behavior thus closely mirror [eos::fst::FileIO](https://gitlab.cern.ch/dss/eos/blob/master/fst/io/FileIo.hh). Prebuild binaries for the library and its dependencies are [available](http://dss-ci-repo.web.cern.ch/dss-ci-repo/kinetic/) for Linux distributions supported by EOS. 


## Table of Contents
 * [Library Overview](#overview)
 * [Dependencies](#dependencies)
 * [Compilation](#compilation)
 * [Installation](#installation)
 * [Versioning](#versioning)
 * [Configuration](#configuration)
 * [Command Line Tool](#command-line-tool)


## Overview 
The main goals are to provide *high performance* and *configurable redundancy* for storing / accessing file data with a file interface (open, read, write, stat, etc.) on top of Kinetic storage devices. 

+ **performance** Multiple drives are grouped into a cluster and file data is distributed among all drives of a cluster. 
+ **redundancy** File data is erasure coded and metadata is replicated within a cluster to meet the configured redundancy level. 

Each drive may belong multiple logical clusters to allow different redundancy and performance configurations with a limited number of drives. Files are assigned to individual clusters and can be accessed with a url-based naming scheme: `kinetic://clustername/path/filename`. 

Every (replicated or erasure encoded) data chunk is stored with a checksum, providing end-to-end reliability. Data corruption is transparently repaired on access (assuming sufficient redundancy). 

Created clusters are independent from one another and static; individual clusters cannot grow / shrink (though drives may be replaced). This property allows key placement within a cluster to be permanent and not require additional metadata. The implementation is correspondingly simple and avoids all complexity inherent to dynamic placement strategies. If a federated namespace for all clusters is required a higher-level namespace implementation, such as provided by EOS, can be employed on top of the kineticio library. 

Cluster health is monitored and a system to identify problematic keys has been implemented to allow repair processes to target specific keys instead of requiring a complete cluster scan. 


## Dependencies
 + Available by package managers:
    + yum ``` cmake gcc-c++ json-c json-c-devel libuuid libuuid-devel ```
    + apt-get ```cmake g++ libjson-c-dev uuid-dev ```
    + brew ``` cmake json-c ossp-uuid ```
 + [isa-l](https://01.org/intel%C2%AE-storage-acceleration-library-open-source-version) intel erasure coding library
 + [kinetic-cpp-client](https://github.com/kinetic/kinetic-cpp-client)

For EOS supported linux distributions, prebuild rpm packages of isa-l and kinetic-cpp-client can be found in the [kineticio-depend](http://dss-ci-repo.web.cern.ch/dss-ci-repo/kinetic/kineticio-depend/) repository. 

## Compilation
The build system uses cmake. The implementation uses c++11 / c++0x features available from gcc 4.4 onwards. 

+ Install dependencies. 
+ Clone this git repository 
+ Create a build directory. If you want you can use the cloned git repository as your build directory, but using a separate directory is recommended in order to cleanly separate sources and generated files. 
+ From your build directory call `cmake /path/to/cloned-git`, if you're using the cloned git repository as your build directory this would be `cmake .` 
+ Run `make`

This will generate the actual library files as well as the [kineticio command-line tool](#command-line-tool). 

To build the test framework

+ additional dependencies are wget, zlib, java
+ run cmake with BUILD_TEST option enabled. e.g. `cmake -DBUILD_TEST=ON .`
+ Run `make`

This will build the kio-test executable in addition to the library and command-line tool. Executing it without arguments will run all tests. For options run with `--help` argument. 

## Installation

For supported fedora / redhat based distributions, yum may be used. If the appropriate [kineticio](http://dss-ci-repo.web.cern.ch/dss-ci-repo/kinetic/kineticio/) and [kineticio-depend](http://dss-ci-repo.web.cern.ch/dss-ci-repo/kinetic/kineticio-depend/) repositories are added to the system, the library and its dependencies can be installed with `yum install kineticio`

For manual installation, run the `make install` command as a superuser after compiling the library. 

## Versioning
The library follows the Major.Minor.Patch versioning scheme, `.so` versioning is done automatically during compilation. 

Changes to the *Major* version indicate that the library interface has changed. Different *Major* versions of the library will not be binary compatible. 

Changes to the *Minor* version number indicate that backwards compatible changes to the interface have ocurred. Usually this means that additional functions available. It resets to zero if the *Major* version changes. 

For the kineticio library the patch version equals the git commit number. It does not reset if the *Major* or *Minor* versions change.  

## Configuration
All configuration has to be provided in json format. It may be generated manually or using the eos admin [web interface](https://github.com/cern-eos/eos-admin-gui).

When using the library with the EOS storage system, the EOS console's `kinetic config --publish` command should be used to distribute configuration changes to all storage servers.

Alternatively, configuration may be provided by setting the following environment variables, specifying drive location, login details and the cluster configuration (as well as library-wide configuration options) respectively.

```
 KINETIC_DRIVE_LOCATION
 KINETIC_DRIVE_SECURITY 
 KINETIC_CLUSTER_DEFINITION 
```
The environment variables may either contain the json configuration directly, or point files containing the respective information. It is possible to have all configuration in a single file and point all environment variables to it. An example configuration (which is used by the library test system) is available [here](test/localhost.json).

In the following, the individual configuration options will be presented. 

|  | Library-wide Configuration Options  |
| --- | --- |
| cacheCapacityMB | The maximum cache size in megabytes. The cache is used to hold data for currently executing operations as well as storing accessed and prefetched data. Minimum cache size can be computed by multiplying the stripe size with the maximum number of concurrent data streams. For a setup with 16-4 erasure coding configuration, 1 MB chunkSize and an expected 20 concurrent data streams, for example, the cache capacity should be at least 400MB (20MB stripe size x 20 streams). Larger capacities allow higher concurrency for writing (asynchronous flushes of multiple data stripes per stream) as well as more traditional caching.
| maxBackgroundIoThreads | The maximum number of background IO threads. If set it defines the limit for concurrent I/O operations (put, get, del). For 10G EOS nodes a value of ~12 achieves good performance. If set to zero, concurrency is controlled by the number of threads employed by the library user. 
| maxBackgroundIoQueue | The maximum number of IO operations queued for execution. If set to 0, background threads will not be held in a pool but use one-shot threads spawned on-demand. For normal operation a value of ~2 times the number of background threads works well.
| maxReadaheadWindow | Limit the maximum readahead to set number of data stripes. Note that the maximum readahead will only be reached if the access pattern is very predictable and there is no cache pressure.

---

| | Cluster Defintion and Configuration |
| --- | --- |
| clusterID | The cluster identifier. As the drive wwn it can be freely chosen but has to be unique. It may **not** contain the ':' or '/' symbols. |
| numData | The number of data chunks that will be stored in a data stripe (required to be >=1). |
| numParity | Defines the redundancy level of this cluster (required to be >=0). Metadata and attribute keys will be stored with numParity replication. Data will be stored in (numData,numParity) erasure coded stripes (unless numData is defined as 1 in which case replication will be used). |
| chunkSizeKB | The maximum size of data chunks in KB (required to be min. 1 and max. 1024). A value of 1024 is optimal for Kinetic drive performance. |
| timeout | Network timeout for cluster operations in seconds. |
| minReconnectInterval | The minimum time / rate limit in seconds between reconnection attempts. |
| drives | A list of wwn identifiers for all drives associated with the cluster. The order of the drives is important and may not be changed after data has been written to the cluster. If a drive is replaced, the new drive wwn has to replace the old drive wwn at the same position. |

Some more information on redundancy and cluster size: 

+ Redundancy is cluster-wide. For example, a cluster with nParity set to 4 can survive up to four concurrent drive failures, but will not be usable after 5 drive failures. 
+ To minimize redundancy overhead, wide stripes are recommended. A (nData,nParity) configuration of (16,4), for example, can sustain four drive failures in the cluster with a 25% redundancy overhead while an (8,4) configuration would provide the same redundancy with a 50% overhead. 
+ The number of drives assigned to a cluster should be least nData + (2 x nParity). Each data and parity chunk is assigned to a unique drive, requiring nData+nParity drives minimum. In case of nParity drive failures in the cluster, having at least nData + (2 x nParity) drives in the cluster allows backup chunks to be written to unique drives as well. Each stripe written in these conditions thus provides the full configured reliability even though some target drives are not accessible at the time of the stripe write. 
+ It is recommended to keep the total number of drives assigned to a single cluster limited (below 50) to limit time required for cluster scan and repair operations, as well as time required for draining a single cluster (if, e.g. all drives are replaced at end of life). 

---

| Name | Drive Location Defintion |
| --- | --- |
| wwn | The world wide name of the drive. While each drive exports a world wide name that can be used for this field, an arbitrary value can be used as long as it is unique within the location definition. |
| inet4 | The ip addresses for both interfaces of the kinetic drive. If you only want to use a single interface then list it twice. |
| port | The port to connect to. The standard port for Kinetic services is 8123. |

The syntax for drive location definition follows the output of the  [kinetic-java-tools](https://github.com/Seagate/kinetic-java-tools) drive discovery process. Drive discovery output may thus be used to generate an initial location definition. 

---

| Name | Drive Security Definition |
| --- | --- |
| wwn | The world wide name of the drive; same as location information. | 
| userId | The user id to use when establishing a connection. |
| key | The secret key / password to use when establishing a connection. |

Drive security is separated from drive location to seperate public knowledge (drive addresses) from private knowledge (drive login). Different users may share a drive location definition but use different login credentials to access the drives. 

## Command Line Tool 

The `kineticio-admin` command line tool provide a variety of admin functions.

```
usage: kinetic --id <name> <operation> [OPTIONS] 

       --id <name> 
           the name of target cluster (see kinetic config)

       <operation> 
           count  : count number of keys existing in the cluster
           scan   : check keys and display their status information
           repair : check keys, repair as required, display key status information
           reset  : force remove keys (Warning: Data will be lost!)
           status : show health status of cluster. 

    OPTIONS
       --target data|metadata|attribute|indicator  
           Operations can be limited to a specific key-type. Setting the 'indicator' type will 
           perform the operation on keys of any type that have been marked as problematic. In 
           most cases this is sufficient and much faster. Use full scan / repair operations 
           after a drive replacement or cluster-wide power loss event. 

       --threads <number> 
           Specify the number of background io threads used for a scan/repair/reset operation. 

       --verbosity debug|notice|warning|error 
           Specify verbosity level. Messages are printed to stdout (warning set as default). 

       --bench <number>
           Only for status operation. Benchmark put/get/delete performance for each  connection 
           using <number> 1MB keys to get rough per-connection throughput. The order of keys is 
           randomized between operations. 

       -m : monitoring key=value output format
```



cmake_minimum_required(VERSION 2.8.6)
project(kineticio CXX C)

################################################################################
# Dependencies
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

find_package(kinetic-c++ REQUIRED)
find_package(jsonc REQUIRED)
find_package(uuid REQUIRED)
find_package(z REQUIRED)
find_package(isal REQUIRED)

################################################################################
# Force /usr/ installation prefix, same as EOS
set(CMAKE_INSTALL_PREFIX /usr)
set(CMAKE_INSTALL_SYSCONFDIR /etc)
set(CMAKE_INSTALL_SBINDIR /sbin)

################################################################################
# Check for Gcc >=4.4
if(CMAKE_COMPILER_IS_GNUCXX)
  exec_program(
      ${CMAKE_CXX_COMPILER}
      ARGS                    --version
      OUTPUT_VARIABLE _compiler_output)
  string(REGEX REPLACE ".* ([0-9]\\.[0-9]\\.[0-9]) .*" "\\1"
	 gcc_compiler_version ${_compiler_output})
  message(STATUS "C++ compiler version: ${gcc_compiler_version} [${CMAKE_CXX_COMPILER}]")

  if(gcc_compiler_version MATCHES "4\\.[0-3]\\.[0-9]")
    message(FATAL_ERROR "Error: you need atleast gcc 4.4.x to compile EOS!")
  endif (gcc_compiler_version MATCHES "4\\.[0-3]\\.[0-9]")
endif (CMAKE_COMPILER_IS_GNUCXX)

################################################################################
# Set CXX flags
if(DEBUG)
    message("Building DEBUG configuration, switch with -DDEBUG=false argument")
    set(CMAKE_CXX_FLAGS "--std=c++0x -g")
else()
    message("Building RELEASE configuration, switch with -DDEBUG=true argument")
    set(CMAKE_CXX_FLAGS "--std=c++0x -O2")
endif(DEBUG)
message("CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

include_directories(
    include/kio
    include
    ${KINETIC-C++_INCLUDE_DIRS}
    ${JSONC_INCLUDE_DIRS}
    ${UUID_INCLUDE_DIRS}
    ${Z_INCLUDE_DIRS}
    ${ISAL_INCLUDE_DIRS}
)
set(kineticio_SRC
    src/ClusterChunkCache.cc
    src/ClusterChunk.cc
    src/ClusterMap.cc
    src/FileIo.cc
    src/FileAttr.cc
    src/Factory.cc
    src/KineticAutoConnection.cc
    src/KineticAsyncOperation.cc
    src/KineticCallbacks.cc
    src/KineticCluster.cc
    src/SocketListener.cc
    src/ErasureCoding.cc
    src/SequencePatternRecognition.cc
    src/BackgroundOperationHandler.cc
    src/Utility.cc
)
set(kineticio_LIB
    ${JSONC_LIBRARIES}
    ${UUID_LIBRARIES}
    ${Z_LIBRARIES}
    ${ISAL_LIBRARIES}
    ${KINETIC-C++_LIBRARIES}
)
add_library(kineticio SHARED
    ${kineticio_SRC}
)
target_link_libraries(kineticio
    ${kineticio_LIB}
)
install(TARGETS kineticio LIBRARY DESTINATION lib64)
install(DIRECTORY ${kineticio_SOURCE_DIR}/include/kio DESTINATION include)

if(DEBUG)
    include(ExternalProject)
    ExternalProject_add(catch
       PREFIX catch
       DOWNLOAD_COMMAND wget -q https://raw.githubusercontent.com/philsquared/Catch/master/single_include/catch.hpp -O catch.hpp
       CONFIGURE_COMMAND ""
       BUILD_IN_SOURCE 1
       BUILD_COMMAND ""
       INSTALL_COMMAND ""
    )
    include_directories(${kineticio_BINARY_DIR}/catch/src)

    add_executable(kinetic-test
        ${kineticio_SRC}
        test/TestMain.cc
        test/ClusterChunkTest.cc
        test/FileIoTest.cc
        test/ClusterMapTest.cc
        test/KineticClusterTest.cc
        test/SocketListenerTest.cc
        test/ErasureCodingTest.cc
        test/UtilityTest.cc
        test/SequencePatternRecognitionTest.cc
        test/SimulatorController.cc
        test/LRUCacheTest.cc
        test/LoggingTest.cc
    )
    target_link_libraries(kinetic-test
       ${kineticio_LIB}
       ${CMAKE_THREAD_LIBS_INIT}
    )
    add_dependencies(kinetic-test catch)
endif(DEBUG)


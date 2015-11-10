cmake_minimum_required(VERSION 2.8.6)
project(kineticio CXX C)
include(ExternalProject)

################################################################################
# Options
option(BUILD_TEST "Build test executables." off)
option(LINK_STATIC "Link kinetic-cpp-client library statically" off)
message(STATUS "Set Options: BUILD_TEST=${BUILD_TEST} LINK_STATIC=${LINK_STATIC}")

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
set(CMAKE_CXX_FLAGS "--std=c++0x -g")

################################################################################
# Dependencies
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

find_package(jsonc REQUIRED)
find_package(uuid REQUIRED)
find_package(Threads REQUIRED)
find_package(kinetic-c++)
find_package(isal)


################################################################################
# Compile dependencies if required
if(LINK_STATIC OR NOT KINETIC-C++_FOUND)
  ExternalProject_add(
    kinetic_cpp_client
    PREFIX "vendor"
    GIT_REPOSITORY "https://github.com/plensing/kinetic-cpp-client"
    GIT_TAG ""
    CMAKE_ARGS -DGOOGLE_STATIC=on
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND ""
  )
  set(KINETIC-C++_INCLUDE_DIRS "${kineticio_BINARY_DIR}/vendor/src/kinetic_cpp_client/include")
  if(LINK_STATIC)
    find_package(OpenSSL REQUIRED)
    find_package(Protobuf REQUIRED)
    find_library(LIBUNWIND "unwind")

    set(KINETIC-C++_LIBRARIES 
      "${kineticio_BINARY_DIR}/vendor/src/kinetic_cpp_client/libkinetic_client_static.a"
      "${kineticio_BINARY_DIR}/vendor/src/kinetic_cpp_client/vendor/lib/libglog.a"
      "${kineticio_BINARY_DIR}/vendor/src/kinetic_cpp_client/vendor/lib/libgflags.a"
      ${OPENSSL_LIBRARIES}
      ${PROTOBUF_LIBRARIES}  
    )        
    if(LIBUNWIND)
      set(KINETIC-C++_LIBRARIES ${KINETIC-C++_LIBRARIES} ${LIBUNWIND})
    endif(LIBUNWIND)
  else(LINK_STATIC)
    set(KINETIC-C++_LIBRARIES 
      "${kineticio_BINARY_DIR}/vendor/src/kinetic_cpp_client/libkinetic_client.so"
    )        
  endif(LINK_STATIC)
endif()

if(NOT ISAL_FOUND)
  # building 2.13 instead of 2.14 because SLC-6 has problems with the 2.14 build system
  ExternalProject_add(
    isal
    PREFIX "vendor"
    URL "https://01.org/sites/default/files/downloads/intelr-storage-acceleration-library-open-source-version/isa-lopensrc2.13.tar.gz"
    URL_MD5 "f21ca06fc07857c71d574e59896c58d7"
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND ""
  )
  set(ISAL_INCLUDE_DIRS "${kineticio_BINARY_DIR}/vendor/src/isal/include")
  set(ISAL_LIBRARIES "${kineticio_BINARY_DIR}/vendor/src/isal/bin/libisal.so")
endif()


include_directories(
    include/kio
    include
    ${KINETIC-C++_INCLUDE_DIRS}
    ${JSONC_INCLUDE_DIRS}
    ${UUID_INCLUDE_DIRS}
    ${ISAL_INCLUDE_DIRS}
)
set(kineticio_SRC
    src/FileIo.cc
    src/FileAttr.cc
    src/Factory.cc
    src/DataBlock.cc
    src/DataCache.cc
    src/ClusterMap.cc
    src/KineticAutoConnection.cc
    src/KineticAsyncOperation.cc
    src/KineticCallbacks.cc
    src/KineticCluster.cc
    src/KineticAdminCluster.cc
    src/SocketListener.cc
    src/ErasureCoding.cc
    src/PrefetchOracle.cc
    src/BackgroundOperationHandler.cc
    src/Utility.cc
    src/outside/crc32c.c
    src/outside/MurmurHash3.cpp
)
set(kineticio_LIB
    ${JSONC_LIBRARIES}
    ${UUID_LIBRARIES}
    ${ISAL_LIBRARIES}
    ${KINETIC-C++_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
)

add_library(kineticio SHARED ${kineticio_SRC})
add_executable(admin ${kineticio_SRC} tools/admin.cc)

if(LINK_STATIC OR NOT KINETIC-C++_FOUND)
  add_dependencies(kineticio kinetic_cpp_client)
  add_dependencies(admin kinetic_cpp_client)
endif()
if(NOT ISAL_FOUND)
  add_dependencies(kineticio isal)
  add_dependencies(admin isal)
endif()

target_link_libraries(kineticio ${kineticio_LIB})
target_link_libraries(admin ${kineticio_LIB} ${CMAKE_THREAD_LIBS_INIT})

install(TARGETS kineticio LIBRARY DESTINATION lib64)
install(DIRECTORY ${kineticio_SOURCE_DIR}/include/kio DESTINATION include)


if(BUILD_TEST)
    ExternalProject_add(catch
       PREFIX catch
       DOWNLOAD_COMMAND wget -q https://raw.githubusercontent.com/philsquared/Catch/master/single_include/catch.hpp -O catch.hpp
       CONFIGURE_COMMAND ""
       BUILD_IN_SOURCE 1
       BUILD_COMMAND ""
       INSTALL_COMMAND ""
    )
    include_directories(${kineticio_BINARY_DIR}/catch/src)

    find_package(z REQUIRED)
    include_directories(${Z_INCLUDE_DIRS})

    ExternalProject_add(
      kinetic_simulator
      PREFIX "vendor"
      GIT_REPOSITORY "https://github.com/kinetic/kinetic-java"
      GIT_TAG "kinetic-java-0.8.0.5"
      CONFIGURE_COMMAND ""
      BUILD_IN_SOURCE 1
      BUILD_COMMAND mvn package -Dmaven.test.skip=true --quiet 
      INSTALL_COMMAND ""
    )

    add_executable(kinetic-test
        ${kineticio_SRC}
        test/TestMain.cc
        test/DataBlockTest.cc
        test/FileIoTest.cc
        test/ClusterMapTest.cc
        test/KineticClusterTest.cc
        test/SocketListenerTest.cc
        test/ErasureCodingTest.cc
        test/UtilityTest.cc
        test/PrefetchOracleTest.cc
        test/SimulatorController.cc
        test/LoggingTest.cc
        test/KineticAdminClusterTest.cc
        test/DataCacheTest.cc
    )
    if(LINK_STATIC OR NOT KINETIC-C++_FOUND)
      add_dependencies(kinetic-test kinetic_cpp_client)
      add_dependencies(kinetic-test kinetic_cpp_client)
    endif()
    if(NOT ISAL_FOUND)
      add_dependencies(kinetic-test isal)
      add_dependencies(kinetic-test isal)
    endif()
    target_link_libraries(kinetic-test
        ${Z_LIBRARIES}
        ${kineticio_LIB}
    )
    add_dependencies(kinetic-test catch)

    add_executable(replay ${kineticio_SRC} test/replay.cc)
    target_link_libraries(replay ${kineticio_LIB} ${CMAKE_THREAD_LIBS_INIT})
  if(LINK_STATIC)
    add_dependencies(replay kinetic_cpp_client)
  endif()
endif(BUILD_TEST)


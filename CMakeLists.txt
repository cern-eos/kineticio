cmake_minimum_required(VERSION 2.8.6)
project(kineticio CXX C)
include(ExternalProject)

################################################################################
# Options
option(BUILD_TEST "Build test executables." off)
option(LINK_STATIC "Link non-standard libraries statically" off)
message(STATUS "Set Options: BUILD_TEST=${BUILD_TEST} LINK_STATIC=${LINK_STATIC}")

################################################################################
# Dependencies
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

find_package(jsonc REQUIRED)
find_package(uuid REQUIRED)
find_package(Threads REQUIRED)
if (LINK_STATIC)
  find_package(OpenSSL REQUIRED)
  find_package(Protobuf REQUIRED)
  find_library(LIBUNWIND "unwind")
else()
  find_package(kinetic-c++ REQUIRED)
  find_package(isal REQUIRED)
endif()


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
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")


################################################################################
# Compile dependencies if LINK_STATIC is set 
if(LINK_STATIC)
  ExternalProject_add(
    kinetic_cpp_client
    PREFIX "vendor"
    GIT_REPOSITORY "https://github.com/plensing/kinetic-cpp-client"
    GIT_TAG ""
    CMAKE_ARGS -DBUILD_STATIC=on
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND ""
  )
  set(KINETIC-C++_INCLUDE_DIRS "${kineticio_BINARY_DIR}/vendor/src/kinetic_cpp_client/include")
  set(KINETIC-C++_LIBRARIES 
    "${kineticio_BINARY_DIR}/vendor/src/kinetic_cpp_client/libkinetic_client.a"
    "${kineticio_BINARY_DIR}/vendor/src/kinetic_cpp_client/vendor/lib/libglog.a"
    "${kineticio_BINARY_DIR}/vendor/src/kinetic_cpp_client/vendor/lib/libgflags.a"
    ${OPENSSL_LIBRARIES}
    ${PROTOBUF_LIBRARIES}
  )        
  if(LIBUNWIND)
      set(KINETIC-C++_LIBRARIES 
          ${KINETIC-C++_LIBRARIES} 
          ${LIBUNWIND}
      )
  endif()

  ExternalProject_add(
    isal
    PREFIX "vendor"
    URL "https://01.org/sites/default/files/downloads/intelr-storage-acceleration-library-open-source-version/isa-l-2.14.0.tar.gz"
    URL_MD5 ""
    CONFIGURE_COMMAND ../isal/configure --prefix=${kineticio_BINARY_DIR}/vendor --enable-static --with-pic
  )
  set(ISAL_INCLUDE_DIRS "${kineticio_BINARY_DIR}/vendor/lib/include")
  set(ISAL_LIBRARIES "${kineticio_BINARY_DIR}/vendor/lib/libisal.a")

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
target_link_libraries(kineticio ${kineticio_LIB})

add_executable(admin ${kineticio_SRC} tools/admin.cc)
target_link_libraries(admin ${kineticio_LIB} ${CMAKE_THREAD_LIBS_INIT})

if(LINK_STATIC)
  add_dependencies(kineticio kinetic_cpp_client)
  add_dependencies(replay kinetic_cpp_client)
  add_dependencies(admin kinetic_cpp_client)
endif()

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
        test/LRUCacheTest.cc
        test/LoggingTest.cc
        test/KineticAdminClusterTest.cc
        test/DataCacheTest.cc
    )
    target_link_libraries(kinetic-test
        ${Z_LIBRARIES}
        ${kineticio_LIB}
    )
    add_dependencies(kinetic-test catch)

    add_executable(replay ${kineticio_SRC} test/replay.cc)
    target_link_libraries(replay ${kineticio_LIB} ${CMAKE_THREAD_LIBS_INIT})

endif(BUILD_TEST)


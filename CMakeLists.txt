cmake_minimum_required(VERSION 2.8.6)
project(kineticio CXX C)

################################################################################
# Options
option(BUILD_TEST "Build test executables." off)
message(STATUS "Set Options: BUILD_TEST=${BUILD_TEST}")

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
find_package(kinetic-c++ REQUIRED)
find_package(isal REQUIRED)
find_package(Threads REQUIRED)
find_package(Git REQUIRED)

################################################################################
# Compile main project 
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
    src/RedundancyProvider.cc
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
install(TARGETS kineticio LIBRARY DESTINATION lib64)
install(DIRECTORY ${kineticio_SOURCE_DIR}/include/kio DESTINATION include)

################################################################################
# Compile cluster admin tool 
add_executable(admin ${kineticio_SRC} tools/admin.cc)
target_link_libraries(admin ${kineticio_LIB} ${CMAKE_THREAD_LIBS_INIT})

################################################################################
# Compile test & test dependencies if requested
if(BUILD_TEST)
    find_package(z REQUIRED)
    include_directories(${Z_INCLUDE_DIRS})
    
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

    if(NOT EXISTS "vendor/src/kinetic_simulator") 
      ExternalProject_add(
        kinetic_simulator
        PREFIX "vendor"
        GIT_REPOSITORY "https://github.com/kinetic/kinetic-java"
        GIT_TAG "kinetic-java-0.8.0.5"
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND mvn package -Dmaven.test.skip=true -T 1C --quiet 
        INSTALL_COMMAND ""
      )
    endif()

    add_executable(kinetic-test
        ${kineticio_SRC}
        test/TestMain.cc
        test/DataBlockTest.cc
        test/FileIoTest.cc
        test/ClusterMapTest.cc
        test/KineticClusterTest.cc
        test/SocketListenerTest.cc
        test/RedundancyProviderTest.cc
        test/UtilityTest.cc
        test/PrefetchOracleTest.cc
        test/SimulatorController.cc
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


################################################################################
# RPM generation rules 

execute_process(
  COMMAND git rev-list HEAD --count
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE GIT_COMMITS
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

set(CPACK_GENERATOR "RPM")
set(CPACK_PACKAGE_CONTACT "paul.lensing@gmail.com")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "A byte-range IO library for a kinetic backend.")
set(CPACK_PACKAGE_VERSION "0.${GIT_COMMITS}")
set(CPACK_RPM_PACKAGE_REQUIRES "isal, kinetic_cpp_client, json-c, libuuid")
set(CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
set(CPACK_PACKAGE_NAME ${CMAKE_PROJECT_NAME})
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_RPM_PACKAGE_ARCHITECTURE}")

include(CPack)
cmake_minimum_required(VERSION 2.8.6)
project(kineticio CXX C)

execute_process(
        COMMAND git log --oneline
        COMMAND wc -l
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_COMMITS
        OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(PROJECT_VERSION_MAJOR 1)
set(PROJECT_VERSION_MINOR 0)
set(PROJECT_VERSION_PATCH ${GIT_COMMITS})
set(PROJECT_VERSION ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH})
message(STATUS "Project version set to ${PROJECT_VERSION}")


################################################################################
# Options
option(BUILD_TEST "Build test executables." off)
message(STATUS "Set Options: BUILD_TEST=${BUILD_TEST}")

################################################################################
# Check for Gcc >=4.4
if (CMAKE_COMPILER_IS_GNUCXX)
    exec_program(
            ${CMAKE_CXX_COMPILER}
            ARGS --version
            OUTPUT_VARIABLE _compiler_output)
    string(REGEX REPLACE ".* ([0-9]\\.[0-9]\\.[0-9]) .*" "\\1"
            gcc_compiler_version ${_compiler_output})
    message(STATUS "C++ compiler version: ${gcc_compiler_version} [${CMAKE_CXX_COMPILER}]")

    if (gcc_compiler_version MATCHES "4\\.[0-3]\\.[0-9]")
        message(FATAL_ERROR "Error: you need atleast gcc 4.4.x to compile EOS!")
    endif (gcc_compiler_version MATCHES "4\\.[0-3]\\.[0-9]")
endif (CMAKE_COMPILER_IS_GNUCXX)

################################################################################
# Set CXX flags
set(CMAKE_CXX_FLAGS "--std=c++0x -g -Wall -Wextra -Wno-unused-function -Wno-unused-parameter -Werror")

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
        src/KineticIoFactory.cc
        src/DataBlock.cc
        src/DataCache.cc
        src/ClusterMap.cc
        src/KineticIoSingleton.cc
        src/KineticAutoConnection.cc
        src/KineticClusterOperation.cc
        src/KineticClusterStripeOperation.cc
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

# Set lib or lib64 depending on architecture
get_property(LIB64 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS)
set(LIBSUFFIX "")
if ("${LIB64}" STREQUAL "TRUE" AND "${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "x86_64")
    set(LIBSUFFIX 64)
endif ()
message(STATUS "Library installation directory is set to ${CMAKE_INSTALL_PREFIX}/lib${LIBSUFFIX}")

set_target_properties(kineticio PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})
install(TARGETS kineticio LIBRARY DESTINATION lib${LIBSUFFIX})
install(DIRECTORY ${kineticio_SOURCE_DIR}/include/kio DESTINATION include)

################################################################################
# Compile cluster admin tool 
add_executable(admin ${kineticio_SRC} tools/admin.cc)
target_link_libraries(admin ${kineticio_LIB} ${CMAKE_THREAD_LIBS_INIT})

################################################################################
# Compile test & test dependencies if requested
if (BUILD_TEST)
    include(ExternalProject)
    find_package(z REQUIRED)

    add_custom_command(OUTPUT ivy.jar
            COMMAND wget -q https://repo.maven.apache.org/maven2/org/apache/ivy/ivy/2.4.0/ivy-2.4.0.jar -O ivy.jar
            COMMAND java -jar ivy.jar -error -dependency com.seagate.kinetic kinetic-all 0.8.0.5 -retrieve 'simulator/[artifact]-[revision] (-[classifier]) .[ext]'
            COMMENT "Downloading simulator"
            )
    add_custom_target(kinetic-simulator DEPENDS ivy.jar)
    add_definitions(-DTESTJSON_LOCATION="${kineticio_SOURCE_DIR}/test/localhost.json")
    add_definitions(-DTESTSIMULATOR_LOCATION="${kineticio_BINARY_DIR}/simulator")

    ExternalProject_add(catch
            PREFIX catch
            DOWNLOAD_COMMAND wget -q https://raw.githubusercontent.com/philsquared/Catch/master/single_include/catch.hpp -O catch.hpp
            CONFIGURE_COMMAND ""
            BUILD_IN_SOURCE 1
            BUILD_COMMAND ""
            INSTALL_COMMAND ""
            )
    include_directories(${Z_INCLUDE_DIRS})
    include_directories(${kineticio_BINARY_DIR}/catch/src)

    add_executable(kio-test
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
            test/KineticAutoConnectionTest.cc
            test/ConcurrencyTest.cc
            test/ConcurrencyAppendTest.cc
            )
    target_link_libraries(kio-test
            ${Z_LIBRARIES}
            ${kineticio_LIB}
            )
    add_dependencies(kio-test catch kinetic-simulator)

    add_executable(replay ${kineticio_SRC} test/replay.cc)
    target_link_libraries(replay ${kineticio_LIB} ${CMAKE_THREAD_LIBS_INIT})
endif (BUILD_TEST)


################################################################################
# RPM generation rules 

set(CPACK_GENERATOR "RPM")
set(CPACK_PACKAGE_CONTACT "paul.lensing@gmail.com")
set(CPACK_PACKAGE_NAME ${CMAKE_PROJECT_NAME})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "A byte-range IO library for a kinetic backend.")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_RPM_PACKAGE_REQUIRES "isal, kinetic_cpp_client >= 0.2.0, json-c, libuuid")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}")
include(CPack)

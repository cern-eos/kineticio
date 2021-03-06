cmake_minimum_required(VERSION 2.8.6)
project(kineticio CXX C)

execute_process(
        COMMAND git log --oneline
        COMMAND wc -l
        COMMAND tr -d [:space:]
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_COMMITS
)
set(PROJECT_VERSION_MAJOR 2)
set(PROJECT_VERSION_MINOR 0)
set(PROJECT_VERSION_PATCH ${GIT_COMMITS})
set(PROJECT_VERSION ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH})
add_definitions(-DKIO_VERSION="${PROJECT_VERSION}")
message(STATUS "Project version set to ${PROJECT_VERSION}")

################################################################################
# Options
option(BUILD_TEST "Build test executables." off)
option(CPACK_HEADER_ONLY "make package builds a headers-only rpm instead of the full package." off)
message(STATUS "Set Options: BUILD_TEST=${BUILD_TEST} CPACK_HEADER_ONLY=${CPACK_HEADER_ONLY}")

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
        message(FATAL_ERROR "Error: you need atleast gcc 4.4.x to compile kineticio")
    endif (gcc_compiler_version MATCHES "4\\.[0-3]\\.[0-9]")
endif (CMAKE_COMPILER_IS_GNUCXX)

################################################################################
# Set CXX flags and preprocessor definitions
set(CMAKE_CXX_FLAGS "--std=c++0x -pthread -g -Wall -Wextra -Wno-unused-function -Wno-unused-parameter")
if(NOT APPLE) # don't error on warning for OSX
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()
add_definitions(-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

################################################################################
# Dependencies
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

find_package(jsonc REQUIRED)
find_package(uuid REQUIRED)
find_package(kinetic-c++ REQUIRED)
find_package(isal REQUIRED)
find_package(Git REQUIRED)
find_package(Threads REQUIRED)

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

# set lib or lib64 as installation directory depending on architecture
get_property(LIB64 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS)
set(LIBSUFFIX "")
if ("${LIB64}" STREQUAL "TRUE" AND "${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "x86_64")
    set(LIBSUFFIX 64)
endif ()
message(STATUS "Library installation directory is set to ${CMAKE_INSTALL_PREFIX}/lib${LIBSUFFIX}")

# set library versioning
set_target_properties(kineticio PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

################################################################################
# Compile cluster admin tool
add_executable(kineticio-admin ${kineticio_SRC} tools/admin.cc)
target_link_libraries(kineticio-admin ${kineticio_LIB} ${CMAKE_THREAD_LIBS_INIT})

################################################################################
# make install targets
install(TARGETS kineticio LIBRARY DESTINATION lib${LIBSUFFIX} COMPONENT library)
install(DIRECTORY ${kineticio_SOURCE_DIR}/include/kio DESTINATION include COMPONENT devel)
install(TARGETS kineticio-admin RUNTIME DESTINATION bin COMPONENT tools)

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

    add_executable(kio-test-dynamic-load test/DynamicLibraryLoadingTest.cc)
    target_link_libraries(kio-test-dynamic-load ${CMAKE_DL_LIBS})
    add_dependencies(kio-test-dynamic-load catch)

    add_executable(replay ${kineticio_SRC} test/replay.cc)
    target_link_libraries(replay ${kineticio_LIB})
endif (BUILD_TEST)


################################################################################
# RPM generation rules
set(CPACK_PACKAGE_NAME ${CMAKE_PROJECT_NAME})
set(CPACK_RPM_PACKAGE_GROUP "Development/Libraries")
set(CPACK_RPM_PACKAGE_LICENSE "Mozilla Public License, v. 2.0")
set(CPACK_PACKAGE_CONTACT "paul.lensing@gmail.com")

if(CPACK_HEADER_ONLY)
    set(CPACK_GENERATOR TGZ RPM)
    set(CPACK_RPM_COMPONENT_INSTALL ON)
    set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
    set(CPACK_COMPONENTS_ALL devel)
    set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR})
    set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "kineticio library headers")
    set(CPACK_RPM_PACKAGE_DESCRIPTION "C++ header files for kineticio library.")
else()
    set(CPACK_GENERATOR RPM)
    set(CPACK_RPM_COMPONENT_INSTALL OFF)
    set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
    set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "kineticio library")
    set(CPACK_RPM_PACKAGE_DESCRIPTION "A byte-range IO library for a kinetic backend.")
    set(CPACK_RPM_PACKAGE_REQUIRES "isal, kinetic_cpp_client >= 0.2.0, json-c, libuuid")
endif(CPACK_HEADER_ONLY)

set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}")
include(CPack)

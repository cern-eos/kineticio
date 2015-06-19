cmake_minimum_required(VERSION 2.8.6)
project(kineticio CXX C)

################################################################################
# Dependencies
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

find_package(kinetic-c++ REQUIRED)
find_package(jsonc REQUIRED)
find_package(uuid REQUIRED)
find_package(z REQUIRED) 

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

include_directories(
    include/kio
    src
    ${KINETIC-C++_INCLUDE_DIRS}
    ${JSONC_INCLUDE_DIRS}
    ${UUID_INCLUDE_DIRS}
    ${Z_INCLUDE_DIRS}
)
add_library(kineticio SHARED
    src/KineticChunk.cc
    src/KineticClusterMap.cc
    src/KineticFileIo.cc
    src/KineticFileAttr.cc
    src/KineticSingletonCluster.cc
   # src/KineticCluster.cc
)

target_link_libraries(kineticio
    ${JSONC_LIBRARIES}
    ${UUID_LIBRARIES}
    ${Z_LIBRARIES}
    ${KINETIC-C++_LIBRARIES}
)

add_executable(kinetic-test
    test/KineticTestMain.cc
    test/KineticChunkTest.cc
    test/KineticIoTest.cc
    test/KineticClusterMapTest.cc
    src/KineticChunk.cc
    src/KineticClusterMap.cc
    src/KineticFileIo.cc
    src/KineticFileAttr.cc
    src/KineticSingletonCluster.cc
)
target_link_libraries(kinetic-test
   ${JSONC_LIBRARIES}
   ${UUID_LIBRARIES}
   ${Z_LIBRARIES}
   ${KINETIC-C++_LIBRARIES}
   ${CMAKE_THREAD_LIBS_INIT}
)

install(TARGETS kineticio LIBRARY DESTINATION lib64)
install(DIRECTORY ${kineticio_SOURCE_DIR}/include/ DESTINATION include)
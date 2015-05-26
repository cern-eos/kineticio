# Try to find kinetic c++ client library
# Once done, this will define
#
# KINETIC-C++_FOUND        - system has kinetic c++ library
# KINETIC-C++_INCLUDE_DIRS - the kinetic include directories
# KINETIC-C++_LIBRARIES    - kinetic libraries

if(KINETIC-C++_INCLUDE_DIRS AND KINETIC-C++_LIBRARIES)
set(KINETIC-C++_FIND_QUIETLY TRUE)
endif(KINETIC-C++_INCLUDE_DIRS AND KINETIC-C++_LIBRARIES)


find_path(KINETIC-C++_INCLUDE_DIR kinetic.h
                           HINTS
			   /usr/include/kinetic/
                           /usr/local/include/kinetic/
			   )

find_library(KINETIC-C++_LIBRARY kinetic_client
			  PATHS /usr/ /usr/local/
                          PATH_SUFFIXES lib lib64
			  )

set(KINETIC-C++_INCLUDE_DIRS ${KINETIC-C++_INCLUDE_DIR})
set(KINETIC-C++_LIBRARIES ${KINETIC-C++_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set KINETIC-C++_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(kinetic-c++ DEFAULT_MSG KINETIC-C++_INCLUDE_DIRS KINETIC-C++_LIBRARIES)

mark_as_advanced(KINETIC-C++_INCLUDE_DIRS KINETIC-C++_LIBRARIES)

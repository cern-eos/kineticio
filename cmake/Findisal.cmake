# Try to find the Intel Storage Acceleration Library
if(ISAL_INCLUDE_DIRS AND ISAL_LIBRARIES)
set(ISAL_FIND_QUIETLY TRUE)
endif(ISAL_INCLUDE_DIRS AND ISAL_LIBRARIES)

find_path(ISAL_INCLUDE_DIR isa-l.h)
find_library(ISAL_LIBRARY isal)

set(ISAL_INCLUDE_DIRS ${ISAL_INCLUDE_DIR})
set(ISAL_LIBRARIES ${ISAL_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set ISAL_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(isal DEFAULT_MSG ISAL_INCLUDE_DIR ISAL_LIBRARY)

mark_as_advanced(ISAL_INCLUDE_DIR ISAL_LIBRARY)

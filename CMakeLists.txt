# File: CMakeLists.txt Author: Michalis Vardoulakis <mvard@ics.forth.gr>

include(cmake_utils/PreventInSouceBuilds.cmake.in)
cmake_minimum_required(VERSION 3.11.0)
include(FetchContent)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
project(kreon VERSION 1.0)

# Set a default build type if none was specified
set(default_build_type "Release")
if(EXISTS "${CMAKE_SOURCE_DIR}/.git")
  set(default_build_type "Debug")
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(
    STATUS
      "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE
      "${default_build_type}"
      CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
                                               "MinSizeRel" "RelWithDebInfo")
endif()

include(GNUInstallDirs)
include(cmake_utils/mkfs.cmake.in)
include(cmake_utils/ycsb.cmake.in)

FetchContent_Declare(log # Recommendation: Stick close to the original name.
                     GIT_REPOSITORY https://github.com/innerout/log.c.git)

FetchContent_GetProperties(log)

if(NOT log_POPULATED)
  FetchContent_Populate(log)
  add_subdirectory(${log_SOURCE_DIR} ${log_BINARY_DIR})
  include_directories(${log_SOURCE_DIR}/include)
  FetchContent_MakeAvailable(log)
endif()

# Compiler configuration
if(NOT CMAKE_C_COMPILER)
  set(CMAKE_C_COMPILER "gcc")
endif()

if(NOT CMAKE_CXX_COMPILER)
  set(CMAKE_CXX_COMPILER "g++")
endif()

include_directories(${CMAKE_BINARY_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/kreon_lib/include)

if(ADD_SANITIZERS)
  set(CMAKE_C_FLAGS "-fsanitize=address,leak,undefined ${CMAKE_C_FLAGS}")
  set(CMAKE_CXX_FLAGS "-fsanitize=address,leak,undefined ${CMAKE_CXX_FLAGS}")
endif()

set(CMAKE_C_FLAGS
    "-std=gnu11 -Wstrict-prototypes -Wall -Wextra -fPIC -fno-omit-frame-pointer -Wunused -Wuninitialized -Wshadow -Wno-address-of-packed-member -Wnull-dereference ${CMAKE_C_FLAGS}"
)
set(CMAKE_C_FLAGS_DEBUG "-ggdb3 -Og ${CMAKE_C_FLAGS_DEBUG}")
set(CMAKE_C_FLAGS_RELEASE
    "${CMAKE_C_FLAGS_RELEASE} -flto -funit-at-a-time -fprefetch-loop-arrays -fpeel-loops -O2 -finline-functions -DNDEBUG"
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++11 -Wall")

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=gnu++11 -Wall")

set(CMAKE_EXE_LINKER_FLAGS
    "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-as-needed -lrt -lm -pthread -lnuma -lutil"
)

add_subdirectory(kreon_lib)
add_subdirectory(tests)
add_subdirectory(tools)
add_subdirectory(YCSB-CXX)

set(CPACK_PACKAGE_NAME ${PROJECT_NAME})
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_VENDOR "FORTH-ICS")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Flash-optimized key-value store")
set(CPACK_GENERATOR "RPM")
set(CPACK_RPM_PACKAGE_LICENSE "ASL v2.0")
set(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")
set(CPACK_RPM_PACKAGE_REQUIRES "numactl-libs >= 2, glibc, libgcc")
set(CPACK_RPM_PACKAGE_DESCRIPTION
    "Kreon is a high-speed and high-efficiency key-value store optimized for Flash storage."
)
set(CPACK_PACKAGING_INSTALL_PREFIX /usr)
set(CPACK_PACKAGE_FILE_NAME
    "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}"
)
include(CPack)
if(NOT TARGET uninstall)
  configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake_utils/cmake_uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake" IMMEDIATE @ONLY)

  add_custom_target(
    uninstall COMMAND ${CMAKE_COMMAND} -P
                      ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
endif()

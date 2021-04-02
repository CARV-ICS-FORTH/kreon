# File: kreon/CMakeLists.txt Author: Michalis Vardoulakis <mvard@ics.forth.gr>

set(KREON_LIB kreon)

set(KREON_C_SOURCE_FILES
    allocator/allocator.c
    allocator/persistent_operations.c
    api/kreon.c
    btree/btree.c
    btree/compaction_daemon.c
    btree/gc.c
    btree/segment_allocator.c
    scanner/min_max_heap.c
    scanner/scanner.c
    scanner/stack.c
    utilities/spin_loop.c
    utilities/list.c)

# set_source_files_properties( KREON_C_SOURCE_FILES PROPERTIES COMPILE_FLAGS
# "${CMAKE_C_FLAGS_RELEASE} ${CMAKE_C_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "-Wl,--no-as-needed -lrt -lm -pthread -lnuma")

add_library(${KREON_LIB} ${KREON_C_SOURCE_FILES})
set_property(TARGET ${KREON_LIB} PROPERTY POSITION_INDEPENDENT_CODE ON)
target_link_libraries(${KREON_LIB} log)
set_source_files_properties(allocator/spin_loop.c PROPERTIES COMPILE_FLAGS
                                                             "-O0")

# mkfs for kreon volumes
add_executable(
  mkfs.kreon
  allocator/mkfs_kreon.c allocator/allocator.c
  allocator/persistent_operations.c btree/segment_allocator.c
  utilities/spin_loop.c utilities/list.c)

target_link_libraries(mkfs.kreon log)
set_target_properties(
  ${KREON_LIB} PROPERTIES PUBLIC_HEADER
                          "${CMAKE_CURRENT_SOURCE_DIR}/include/kreon.h")

install(
  TARGETS ${KREON_LIB} mkfs.kreon
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
set_target_properties(${KREON_LIB} PROPERTIES OUTPUT_NAME ${KREON_LIB})

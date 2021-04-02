# File: kreon/CMakeLists.txt Author: Michalis Vardoulakis <mvard@ics.forth.gr>

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set_source_files_properties(test_scans.c PROPERTIES COMPILE_FLAGS "-O3 -w")

set_source_files_properties(test_iterators.c PROPERTIES COMPILE_FLAGS "-O3 -w")

set(CMAKE_EXE_LINKER_FLAGS "-Wl,-no-as-needed -lm -lrt -pthread -lnuma")

add_executable(test_scans test_scans.c)
target_link_libraries(test_scans kreon)

add_executable(test_iterators test_iterators.c)
target_link_libraries(test_iterators kreon)

add_executable(test_allocator test_allocator.c)
target_link_libraries(test_allocator kreon)

add_executable(test_fmap test_fmap.c)
target_link_libraries(test_fmap kreon)

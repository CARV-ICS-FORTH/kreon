# File: YCSB-CXX/CMakeLists.txt Author: Michalis Vardoulakis
# <mvard@ics.forth.gr>

include_directories(BEFORE core db ../kreon_lib)

add_executable(ycsb-edb db/db_factory_kreondb.cc core/core_workload.cc
                        driver/ycsbc.cc)

target_link_libraries(ycsb-edb kreon)
set_target_properties(ycsb-edb PROPERTIES LINK_FLAGS "-pthread -lnuma")

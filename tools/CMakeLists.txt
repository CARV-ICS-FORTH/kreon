set_source_files_properties(kv_dumper.c PROPERTIES COMPILE_FLAGS "-O2")

add_executable(kv_dumper kv_dumper.c)
target_link_libraries(kv_dumper kreon)

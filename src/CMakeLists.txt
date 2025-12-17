add_library(db_core)

target_include_directories(db_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_subdirectory(buffer)
add_subdirectory(storage)

add_executable(db
    main.cpp
)

target_link_libraries(db PRIVATE db_core)

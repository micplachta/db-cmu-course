add_executable(DB_PROJECT
    main.cpp
)

target_include_directories(DB_PROJECT PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_subdirectory(buffer)
add_subdirectory(storage)


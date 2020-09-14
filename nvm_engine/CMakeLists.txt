project(nvm_engine)

set(src
        ${CMAKE_CURRENT_SOURCE_DIR}/NvmEngine.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/NvmEngine.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/Statement.hpp)

include_directories(
        ${CMAKE_SOURCE_DIR}/include
)

add_executable(${PROJECT_NAME} ${src})

target_link_libraries(${PROJECT_NAME} db)

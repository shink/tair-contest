project(db)

set(src ${CMAKE_CURRENT_SOURCE_DIR}/db.hpp)

add_library(${PROJECT_NAME} STATIC ${src})

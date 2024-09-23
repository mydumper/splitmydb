cmake_minimum_required(VERSION 2.6)
project(mydumper)
set(VERSION 0.12.4)
set(ARCHIVE_NAME "${CMAKE_PROJECT_NAME}-${VERSION}")

#Required packages
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/modules)
find_package(MySQL)
find_package(ZLIB)
find_package(GLIB2)
find_package(PCRE)
find_package(OpenSSL)

if (NOT OpenSSL_FOUND)
  option(WITH_SSL FALSE)
  string(REPLACE "MYSQL_LIBRARIES_ssl-NOTFOUND"    "" MYSQL_LIBRARIES "${MYSQL_LIBRARIES}")
  string(REPLACE "MYSQL_LIBRARIES_crypto-NOTFOUND" "" MYSQL_LIBRARIES "${MYSQL_LIBRARIES}")
endif (NOT OpenSSL_FOUND)


option(WITH_SSL "Build SSL support" ON)
option(WITH_ZSTD "Build ZSTD support" OFF)
if (WITH_ZSTD)
  find_package(ZSTD)
endif (WITH_ZSTD)

if (WITH_ZSTD)
  set(CMAKE_C_FLAGS "-Wall -Wno-deprecated-declarations -Wunused -Wwrite-strings -Wno-strict-aliasing -Wextra -Wshadow -O3 -g -DZWRAP_USE_ZSTD=1 -Werror -Wno-discarded-qualifiers ${MYSQL_CFLAGS}")
  include_directories(${MYDUMPER_SOURCE_DIR} ${MYSQL_INCLUDE_DIR} ${GLIB2_INCLUDE_DIR} ${PCRE_INCLUDE_DIR} ${ZLIB_INCLUDE_DIRS} ${ZSTD_INCLUDE_DIR} )
else (WITH_ZSTD)
  set(CMAKE_C_FLAGS "-Wall -Wno-deprecated-declarations -Wunused -Wwrite-strings -Wno-strict-aliasing -Wextra -Wshadow -O3 -g -Werror ${MYSQL_CFLAGS}")
  include_directories(${MYDUMPER_SOURCE_DIR} ${MYSQL_INCLUDE_DIR} ${GLIB2_INCLUDE_DIR} ${PCRE_INCLUDE_DIR} ${ZLIB_INCLUDE_DIRS} )
endif (WITH_ZSTD)

if (NOT CMAKE_INSTALL_PREFIX)
  SET(CMAKE_INSTALL_PREFIX "/usr/local" CACHE STRING "Install path" FORCE)
endif (NOT CMAKE_INSTALL_PREFIX)
MARK_AS_ADVANCED(CMAKE)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_SOURCE_DIR}/src/config.h )
SET( ZSTD_SRCS zstd/zstd_zlibwrapper.c zstd/gzclose.c zstd/gzlib.c zstd/gzread.c zstd/gzwrite.c )
SET( SPLITMYDB_SRCS src/splitmydb.c src/connection.c src/write_insert.c src/tables_skiplist.c src/common.c)

if (WITH_ZSTD)
  add_executable(splitmydb ${SPLITMYDB_SRCS} ${ZSTD_SRCS})
  target_link_libraries(splitmydb ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${GIO2_LIBRARIES} ${GOBJECT2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES} ${ZSTD_LIBRARIES} stdc++ m )

else (WITH_ZSTD)
  add_executable(splitmydb ${SPLITMYDB_SRCS})
  target_link_libraries(splitmydb ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${GIO2_LIBRARIES} ${GOBJECT2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES} stdc++ m )

endif (WITH_ZSTD)

INSTALL(TARGETS splitmydb
  RUNTIME DESTINATION bin
)

MESSAGE(STATUS "------------------------------------------------")
MESSAGE(STATUS "MYSQL_CONFIG = ${MYSQL_CONFIG}")
MESSAGE(STATUS "CMAKE_INSTALL_PREFIX = ${CMAKE_INSTALL_PREFIX}")
MESSAGE(STATUS "WITH_ZSTD = ${WITH_ZSTD}")
MESSAGE(STATUS "OpenSSL_FOUND = ${OpenSSL_FOUND}")
MESSAGE(STATUS "WITH_SSL = ${WITH_SSL}")
MESSAGE(STATUS "Change a values with: cmake -D<Variable>=<Value>")
MESSAGE(STATUS "------------------------------------------------")
MESSAGE(STATUS)

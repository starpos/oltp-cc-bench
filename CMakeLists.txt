cmake_minimum_required(VERSION 3.0)

project(oltp-cc-bench LANGUAGES CXX)

message(STATUS "Build Type: " ${CMAKE_BUILD_TYPE})
add_definitions("-Wall -Wextra -std=c++17")

set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING "build type")
set(CMAKE_CXX_COMPILER             "clang++" CACHE STRING "CXX compiler")
set(CMAKE_CXX_FLAGS                "-pthread")
set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g")
set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE        "-Ofast -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-Ofast -DNDEBUG -g")

# For rtags
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(MUTEX_ON_CACHELINE ON CACHE BOOL "mutex on cacheline")
set(NO_PAYLOAD OFF CACHE BOOL "payload size 0")
set(LTO ON CACHE BOOL "use LTO")


# Get compiler type.
execute_process(COMMAND sh "-c" "${CMAKE_CXX_COMPILER} --version | head -n1 | cut -d ' ' -f 1" OUTPUT_VARIABLE tmp1)
string(STRIP ${tmp1} tmp2)
message(STATUS "CXX compilerType: ${tmp2}")
if(tmp2 STREQUAL clang)
	set(compilerType clang)
else()
	set(compilerType gcc)
endif()
message(STATUS "compilerType: " ${compilerType})

# Version (for gcc only)
execute_process(COMMAND sh "-c" "${CMAKE_CXX_COMPILER} -dumpversion |cut -d '.' -f 1" OUTPUT_VARIABLE tmp1)
string(STRIP ${tmp1} gcc_major_version)
message(STATUS "gcc_major_version: " ${gcc_major_version})

# Get architecture
execute_process(COMMAND uname -m COMMAND tr -d '\n' OUTPUT_VARIABLE architecture)
message(STATUS "architecture: " ${architecture})


set(cflagItems "")
set(ldflagItems "")
message(STATUS "MUTEX_ON_CACHELINE: " ${MUTEX_ON_CACHELINE})
if(MUTEX_ON_CACHELINE)
	list(APPEND cflagItems " -DMUTEX_ON_CACHELINE")
endif()
message(STATUS "NO_PAYLOAD: " ${NO_PAYLOAD})
if(NO_PAYLOAD)
	list(APPEND cflagItems " -DNO_PAYLOAD")
endif()
message(STATUS "LTO: " ${LTO})
if(LTO)
	list(APPEND cflagItems " -flto=thin")
endif()

if(architecture STREQUAL x86_64)
	list(APPEND cflagItems " -mcx16")
	#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcx16")
endif()

if((NOT compilerType STREQUAL clang) AND (gcc_major_version GREATER_EQUAL 7))
	set(useAtomicLibrary ON)
else()
	set(useAtomicLibrary OFF)
endif()

list(APPEND ldflagItems " -Wl,-R,'$$ORIGIN' -L./")

string(CONCAT cflagsStr ${cflagItems})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${cflagsStr}")
message(STATUS "CXXFLAGS: " ${CMAKE_CXX_FLAGS})

string(CONCAT ldflagsStr ${ldflagItems})
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${ldflagsStr}")
message(STATUS "LDFLAGS: " ${CMAKE_EXE_LINKER_FLAGS})


set(CMAKE_VERBOSE_MAKEFILE 1)
include_directories(${PROJECT_SOURCE_DIR}/include ${PROJECT_SOURCE_DIR}/cybozulib/include)
file(GLOB cppFiles RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

foreach(cppFile ${cppFiles})
	message(STATUS "Target ${cppFile}")
	string(REPLACE ".cpp" "" binFile ${cppFile})
	add_executable(${binFile} ${cppFile})
	if(useAtomicLibrary)
		target_link_libraries(${binFile} atomic)
	endif()
endforeach(cppFile)


add_custom_target(rebuild ninja clean COMMAND ninja USES_TERMINAL)

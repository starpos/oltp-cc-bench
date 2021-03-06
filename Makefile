CXX = g++
CXX_KIND = $(shell $(CXX) --version |head -n1 |cut -d ' ' -f 1)
CXX_VERSION_GE_7 = $(shell test `$(CXX) -dumpversion |cut -d '.' -f 1` -ge 7 && echo true || echo false)
ARCH = $(shell uname -m)

ifeq ($(DEBUG),1)
    CFLAGS = -O0 -g -DDEBUG
    CMAKE_TARGET = Debug
else
    #CFLAGS = -O2 -g -DNDEBUG -mcx16 -march=native
    #CFLAGS = -Ofast -g -DNDEBUG -mcx16 -march=native
    #CFLAGS = -Ofast -g -DNDEBUG -mcx16
    CFLAGS = -Ofast -g -DNDEBUG -ftree-vectorize
    CMAKE_TARGET = RelWithDebInfo
endif

ifeq ($(LTO),1)
    CFLAGS += -flto=thin
endif

ifneq ($(MUTEX_ON_CACHELINE),0)
    CFLAGS += -DMUTEX_ON_CACHELINE
endif

ifeq ($(NO_PAYLOAD),1)
    CFLAGS += -DNO_PAYLOAD
endif

ifeq ($(ARCH),x86_64)
    CFLAGS += -mcx16
endif

#CFLAGS +=  -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free

CFLAGS += -Wall -Wextra -I./include -I./cybozulib/include
CXXFLAGS = -std=c++17 -pthread $(CFLAGS)
#LDFLAGS = -latomic
LDFLAGS = -Wl,-R,'$$ORIGIN' -L./
#LDLIBS = -ltcmalloc_minimal
#LDLIBS = -ltcmalloc
#LDLIBS = libtcmalloc.a
#LDLIBS = -ljemalloc
LDLIBS =

ifeq ($(STATIC),1)
  ifeq ($(CXX_KIND),clang)
    LDFLAGS += -static
    LDLIBS += -lc++abi -lunwind -ldl
  else
    LDFLAGS += -static -static-libgcc -static-libstdc++
    LDLIBS += -Wl,--whole-archive -lpthread -lrt -Wl,--no-whole-archive
  endif
endif


# Compiler specific options.
ifeq ($(CXX_KIND),clang)
  #CXXFLAGS += -stdlib=libc++
  #LDFLAGS += --rtlib=compiler-rt
  ifeq ($(ARCH),aarch64)
    #LDFLAGS += -fuse-ld=lld
  endif
else
  ifeq ($(findstring g++,$(CXX_KIND)),g++)
    ifeq ($(CXX_VERSION_GE_7),true)
        LDLIBS += -latomic  # atomic builtin for 128bit requires libatomic from gcc-7.
    endif
  endif
endif


SOURCES = $(wildcard *.cpp)
DEPENDS = $(patsubst %.cpp,%.d,$(SOURCES))
BINARIES = $(patsubst %.cpp,%,$(SOURCES))

all: $(BINARIES)

rebuild:
	$(MAKE) clean
	$(MAKE) all

.SUFFIXES:
.SUFFIXES: .depend .cpp .o

%: %.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LDLIBS)

.cpp.o:
	$(CXX) $(CXXFLAGS) -c $< -MMD -MP

clean:
	rm -f $(BINARIES) *.o *.d

cmake:
	cmake -G Ninja . -DCMAKE_BUILD_TYPE=$(CMAKE_TARGET) -DCMAKE_CXX_COMPILER=$(CXX)
cmake_clean:
	rm -rf CMakeFiles CMakeCache.txt cmake_install.cmake ninja.* .ninja_*
cmake_rebuild:
	$(MAKE) cmake_clean
	$(MAKE) cmake
cmake_all:
	for CMAKE_TARGET in Debug RelWithDebInfo; do \
		$(MAKE) cmake_clean; \
		cmake -G Ninja . -DCMAKE_BUILD_TYPE=$$CMAKE_TARGET -DCMAKE_CXX_COMPILER=$(CXX); \
		ninja; \
	done


ifeq "$(findstring $(MAKECMDGOALS), clean)" ""
-include $(DEPENDS)
endif

.SECONDARY: $(patsubst %.cpp,%.o,$(SOURCES))

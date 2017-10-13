CXX = g++
CXX_KIND = $(shell $(CXX) --version |head -n1 |cut -d ' ' -f 1)
CXX_VERSION_GE_7 = $(shell test `$(CXX) -dumpversion |cut -d '.' -f 1` -ge 7 && echo true || echo false)

ifeq ($(DEBUG),1)
    CFLAGS = -O0 -g -DDEBUG -mcx16
else
    #CFLAGS = -O2 -g -DNDEBUG -mcx16 -march=native
    #CFLAGS = -Ofast -g -DNDEBUG -mcx16 -march=native
    #CFLAGS = -Ofast -g -DNDEBUG -mcx16
    CFLAGS = -Ofast -g -DNDEBUG -mcx16 -ftree-vectorize
endif

ifeq ($(LTO),1)
    CFLAGS += -flto=thin
endif

ifneq ($(MUTEX_ON_CACHELINE),0)
    CFLAGS += -DMUTEX_ON_CACHELINE
endif

#CFLAGS +=  -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free

CFLAGS += -Wall -Wextra -I./include -I./cybozulib/include
CXXFLAGS = -std=c++1z -pthread $(CFLAGS)
#LDFLAGS = -latomic
LDFLAGS = -Wl,-R,'$$ORIGIN' -L./
#LDLIBS = -ltcmalloc_minimal
#LDLIBS = -ltcmalloc
#LDLIBS = libtcmalloc.a
#LDLIBS = -ljemalloc

ifeq ($(STATIC),1)
LDFLAGS += -static -static-libgcc -static-libstdc++
LDLIBS = -Wl,--whole-archive -lpthread -lrt -Wl,--no-whole-archive
endif

# Compiler specific options.
ifeq ($(CXX_KIND),clang)
  CXXFLAGS += -stdlib=libc++
  LDFLAGS += --rtlib=compiler-rt
else
  ifeq ($(findstring g++,$(CXX_KIND)),g++)
    ifeq ($(CXX_VERSION_GE_7),true)
        LDLIBS += -latomic  # atomic builtin for 128bit requires libatomic from gcc-7.
        CFLAGS += -fno-new-ttp-matching  # without this, cybozulib/option does not work well with -std=c++17.
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

ifeq "$(findstring $(MAKECMDGOALS), clean)" ""
-include $(DEPENDS)
endif

.SECONDARY: $(patsubst %.cpp,%.o,$(SOURCES))

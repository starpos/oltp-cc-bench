CXX = g++

ifeq ($(DEBUG),1)
    CFLAGS = -O0 -g -DDEBUG -mcx16
else
    #CFLAGS = -O2 -g -DNDEBUG -mcx16 -march=native
    #CFLAGS = -Ofast -g -DNDEBUG -mcx16 -march=native
    CFLAGS = -Ofast -g -DNDEBUG -mcx16
endif

ifeq ($(MUTEX_ON_CACHELINE),0)
else
    CFLAGS += -DMUTEX_ON_CACHELINE
endif

#CFLAGS +=  -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free

CFLAGS += -Wall -Wextra -I./include -I./cybozulib/include
CXXFLAGS = -std=c++14 -pthread $(CFLAGS)
#LDFLAGS = -latomic
LDFLAGS = -Wl,-R,'$$ORIGIN' -L./
#LDLIBS = -ltcmalloc_minimal
#LDLIBS = -ltcmalloc
#LDLIBS = libtcmalloc.a

ifeq ($(STATIC),1)
LDFLAGS += -static -static-libgcc -static-libstdc++
LDLIBS = -Wl,--whole-archive -lpthread -lrt -Wl,--no-whole-archive
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

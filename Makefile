V= 1.0.0
CONFIG= ./config

include $(CONFIG)

OBJS= src/luanosql.o src/lns_$T.o


SRCS= src/luanosql.h src/luanosql.c \
      src/lns_unqlite.c

AR= ar rcu
RANLIB= ranlib


lib: src/$(LIBNAME)

src/$(LIBNAME): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(LIB_OPTION) $(OBJS) $(DRIVER_LIBS)

install:
	mkdir -p $(LUA_LIBDIR)/luanosql
	cp src/$(LIBNAME) $(LUA_LIBDIR)/luanosql

clean:
	rm -f src/$(LIBNAME) src/*.o

package = "LuaNoSQL-UnQLite"
version = "cvs-1"
source = {
  url = "git://github.com/hcsturix74/luanosql.git"
}
description = {
   summary = "NoSQL Database connectivity for Lua (UnQLite driver)",
   detailed = [[
      LuaNoSQL is a simple no-ffi-based interface from Lua to NoSQL DBMS. It enables a
      Lua program to connect to NoSQL databases (such as UnQLite or Vedis), execute 
	  arbitrary key/value operations and manage data using cursors.
   ]],
   license = "MIT/X11",
   homepage = "http://github.com/hcsturix74/LuaNoSQL"
}
dependencies = {
   "lua >= 5.1"
}
external_dependencies = {
   UNQLITE = {
      header = "unqlite.h"
   }
}
build = {
   type = "builtin",
   modules = {
     ["luanosql.unqlite"] = {
       sources = { "src/luanosql.c", "src/lns_unqlite.c" },
       libraries = { "unqlite" },
       incdirs = { "$(UNQLITE_INCDIR)" },
       libdirs = { "$(UNQLITE_LIBDIR)" }
     }
   }
}
# LuaNoSQL
LuaNoSQL is a simple no-ffi-based interface from Lua to NoSQL DBMS (UnQLite, Vedis...)

## Overview

LuaNoSQL is a simple interface from Lua to NoSQL DBMS (heavily inspired by [LuaSQL](http://keplerproject.github.io/luasql/doc/us/) & [LuaSQLite3](http://lua.sqlite.org/index.cgi/index)). 
This implementation is NOT based on [FFI library](http://luajit.org/ext_ffi.html).

It enables a Lua program to:
 * Connect to UnQLite, Vedis (not supported yet) databases;
 * Execute arbitrary key/value operations;
 * Manage data using cursors.
 
Version 1.0.0 is the first official release and it supports [UnQLite](http://unqlite.org) DB only.

JX9 Document Store functions for UnQLite are currently not supported (in development)

## Status

LuaNoSQL version 1.0.0 (for Lua 5.1 and 5.2) is now available. 
It can be compiled under Linux and OS X with current Makefile.
It should also work in Windows OSes but a custom Makefile should be added.


## Dependencies

LuaNoSQL depends on Lua 5.x and on the corresponding database library (UnQLite, Vedis,...).
You can compile it from sources under Linux or OS X with GCC compiler.

## Installation

Check in Makefile that installation folder is correct for your linux or OS X system.
To install LuaNoSQL:
 * To compile it, go to LuaNoSQL folder and type:
 ```
 make lib
 ```

 * Then, to install it, type:
 ```
 make install
 ```
 
 You are done! You can use it


## Example:

Here a simple example on how to use LuaNoSQL with UnQLite driver.
For feature list and supported functions check the documentation.

```lua

-- load driver
driver = require"luanosql.unqlite"

-- create environment object
env = driver.unqlite()

-- connect to unqlite data source
con = env:connect("luaunqlite-test")

-- insert a key/value element
res = con:kvstore("key1","Hello World!")

-- retrieve data
res, data = con:kvfetch("key1")
print("Data for key1 = ", data)

```
 
## Tests

In tests folder you can see some examples and tests.
Tests are written using lua [telescope](https://github.com/norman/telescope) and can be run from bash: 

```bash

tsc -f tests/unqlite_teletests.lua

```

## License

LuaNoSQL is free software and uses the same [license](https://github.com/hcsturix74/LuaNoSQL/blob/master/LICENSE)
as Lua 5.1.


## In Development

Version 1.1.0:

* JX9 DOC STORE API: expose document  store functions for built-in JX9 engine

Version 1.2.0:

* Vedis Driver: an interface for Vedis NoSQL DBMS



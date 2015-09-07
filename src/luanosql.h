
#ifndef _LUANOSQL_
#define _LUANOSQL_

#ifndef LUANOSQL_API
#define LUANOSQL_API
#endif

#if !defined LUA_VERSION_NUM

#define lua_pushinteger(L, n) \
	lua_pushnumber(L, (lua_Number)n)
#endif

#define LUANOSQL_PREFIX "LuaNoSQL: "
#define LUANOSQL_TABLENAME "luanosql"
#define LUANOSQL_ENVIRONMENT "Each driver must have an environment metatable"
#define LUANOSQL_CONNECTION "Each driver must have a connection metatable"
#define LUANOSQL_CURSOR "Each driver must have a cursor metatable"

LUANOSQL_API int luanosql_faildirect (lua_State *L, const char *err);
LUANOSQL_API int luanosql_failmsg (lua_State *L, const char *err, const char *m);
LUANOSQL_API int luanosql_createmeta (lua_State *L, const char *name, const luaL_Reg *methods);
LUANOSQL_API void luanosql_setmeta (lua_State *L, const char *name);
LUANOSQL_API void luanosql_set_info (lua_State *L);

#if !defined LUA_VERSION_NUM || LUA_VERSION_NUM==501
void luaL_setfuncs (lua_State *L, const luaL_Reg *l, int nup);
#endif

#endif

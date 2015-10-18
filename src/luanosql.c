/*
** LuaUnQLite binding
** LuaUnQlite no-FFI based binding
** Author: Luca Sturaro (hcsturix74(at)gmail.com)
** See Copyright Notice in license.html
** Taken from LuaSQL and adapted
** 
** Credits:
** Thanks to LuaSQLite3 and LuaSQL projects
** This binding is heavily inspired by them and
** helped me to get into "lua/C binding world".
*/

#include <string.h>

#include "lua.h"
#include "lauxlib.h"


#include "luanosql.h"

#if !defined(lua_pushliteral)
#define lua_pushliteral(L, s) \
	lua_pushstring(L, "" s, (sizeof(s)/sizeof(char))-1)
#endif


/*
** Typical database error situation
*/
LUANOSQL_API int luanosql_faildirect(lua_State *L, const char *err) {
    lua_pushnil(L);
    lua_pushliteral(L, LUANOSQL_PREFIX);
    lua_pushstring(L, err);
    lua_concat(L, 2);
    return 2;
}


/*
** Database error with LuaNoSQL message
** @param L lua state
** @param err LuaNoSQL error message.
** @param m Driver error message.
*/
LUANOSQL_API int luanosql_failmsg(lua_State *L, const char *err, const char *m) {
    lua_pushnil(L);
    lua_pushliteral(L, LUANOSQL_PREFIX);
    lua_pushstring(L, err);
    lua_pushstring(L, m);
    lua_concat(L, 3);
    return 2;
}


typedef struct {
    short  closed;
} pseudo_data;

/*
** Return the name of the object's metatable.
** This function is used by `tostring'.
*/
static int luanosql_tostring (lua_State *L) {
    char buff[100];
    pseudo_data *obj = (pseudo_data *)lua_touserdata (L, 1);
    if (obj->closed)
        strcpy (buff, "closed");
    else
        sprintf (buff, "%p", (void *)obj);
    lua_pushfstring (L, "%s (%s)", lua_tostring(L,lua_upvalueindex(1)), buff);
    return 1;
}


#if !defined LUA_VERSION_NUM || LUA_VERSION_NUM==501
/*
** Adapted from Lua 5.2.0
*/
void luaL_setfuncs (lua_State *L, const luaL_Reg *l, int nup) {
    luaL_checkstack(L, nup+1, "too many upvalues");
    for (; l->name != NULL; l++) {	/* fill the table with given functions */
        int i;
        lua_pushstring(L, l->name);
        for (i = 0; i < nup; i++)	/* copy upvalues to the top */
            lua_pushvalue(L, -(nup + 1));
        lua_pushcclosure(L, l->func, nup);	/* closure with those upvalues */
        lua_settable(L, -(nup + 3));
    }
    lua_pop(L, nup);	/* remove upvalues */
}
#endif

/*
** Create a metatable and leave it on top of the stack.
** @param L Lua state
** @param name of the metatable to be created
** @param methods a luaL_Reg pointer containing the exposed methods 
** @return int the table
*/
LUANOSQL_API int luanosql_createmeta (lua_State *L, const char *name, const luaL_Reg *methods) {
    if (!luaL_newmetatable (L, name))
        return 0;

    /* define methods */
    luaL_setfuncs (L, methods, 0);

    /* define metamethods */
    lua_pushliteral (L, "__index");
    lua_pushvalue (L, -2);
    lua_settable (L, -3);

    lua_pushliteral (L, "__tostring");
    lua_pushstring (L, name);
    lua_pushcclosure (L, luanosql_tostring, 1);
    lua_settable (L, -3);

    lua_pushliteral (L, "__metatable");
    lua_pushliteral (L, LUANOSQL_PREFIX"you're not allowed to get this metatable");
    lua_settable (L, -3);

    return 1;
}


/*
** Define the metatable for the object on top of the stack
** @param L Lua state
** @param name metatable name
** @return void
*/
LUANOSQL_API void luanosql_setmeta (lua_State *L, const char *name) {
    luaL_getmetatable (L, name);
    lua_setmetatable (L, -2);
}


/*
** Assumes the table is on top of the stack.
** @param L Lua state
** @return void
*/
LUANOSQL_API void luanosql_set_info (lua_State *L) {
    lua_pushliteral (L, "_COPYRIGHT");
    lua_pushliteral (L, "Copyright (C) 2015 Luca Sturaro");
    lua_settable (L, -3);
    lua_pushliteral (L, "_DESCRIPTION");
    lua_pushliteral (L, "LuaNoSQL is a simple interface from Lua to NoSQL DBMS.");
    lua_settable (L, -3);
    lua_pushliteral (L, "_VERSION");
    lua_pushliteral (L, "LuaNoSQL 1.0.0");
    lua_settable (L, -3);
}

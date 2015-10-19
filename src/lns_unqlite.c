/*
** LuaUnQLite binding
** LuaUnQlite no-FFI based binding
** Author: Luca Sturaro (hcsturix74(at)gmail.com)
** See Copyright Notice in license.html
**
** Credits:
** Thanks to LuaSQLite3 and LuaSQL projects
** This binding is heavily inspired by them and
** helped me to get into "lua/C binding world".
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "unqlite.h"

#include "lua.h"
#include "lauxlib.h"


#include "luanosql.h"



#define LUANOSQL_ENVIRONMENT_UNQLITE "UnQLite environment"
#define LUANOSQL_CONNECTION_UNQLITE "UnQLite connection"
#define LUANOSQL_CURSOR_UNQLITE "UnQLite cursor"


/* Environment data structure */
typedef struct
{
    short   closed;             /**< env closed or not */
} env_data;

/* Connection data structure */
typedef struct
{
    short        closed;               /**< conn closed or not */
    int          env;                  /**< reference to environment */
    unsigned int cur_counter;	       /**< cursor counter (incremented or decremented) */
    unqlite      *unqlite_conn;        /**< database connection unqlite */
    int 		 con_fetch_cb;         /**< reference to unqlite_kv_fetch_callback */
    int 		 con_fetch_cb_udata;   /**< reference to unqlite_kv_fetch_callback userdata*/
    lua_State    *L;                   /**< refernce to a lua_state, useful for callback implementation */
} conn_data;

/* Cursor data structure */
typedef struct
{
    short       closed;
    int         conn;               /**< reference to connection */
    conn_data   *conn_data;         /**< reference to connection data structure */
    unqlite_kv_cursor *cursor;	    /**< reference to unqlite_kv_cursor struct */
    int cur_key_cb;                 /**< reference to unqlite_kv_cursor_key_callback - not used now */
    int cur_key_cb_udata;           /**< reference to unqlite_kv_cursor_key_callback userdata - not used now */
    int cur_data_cb;                /**< reference to unqlite_kv_cursor_data_callback - not used now*/
    int cur_data_cb_udata;          /**< reference to unqlite_kv_cursor_data_callback userdata - not used now */
} cur_data;

/* LUANOSQL_API function */
LUANOSQL_API int luaopen_luanosql_unqlite(lua_State *L);


#ifdef LUANOSQL_DEBUG
/** This is a helper function for debug Lua stack */
static void stackDump (lua_State *L) {
    int i=lua_gettop(L);
    printf(" ----------------  Stack Dump ----------------\n" );
    while( i )
    {
        int t = lua_type(L, i);
        switch (t)
        {
        case LUA_TSTRING:
            printf("%d:%s\n", i, lua_tostring(L, i));
            break;
        case LUA_TBOOLEAN:
            printf("%d: %s\n",i,lua_toboolean(L, i) ? "true" : "false");
            break;
        case LUA_TNUMBER:
            printf("%d: %g\n",  i, lua_tonumber(L, i));
            break;
        default:
            printf("%d: %s\n", i, lua_typename(L, t));
            break;
        }
        i--;
    }
    printf("--------------- Stack Dump Finished ---------------\n" );
}
#endif  //End LUANOSQL_DEBUG
/**
** Create a metatable and leave it on top of the stack.
** @param conn a connection to unqlite db
** @param zBuf a string where the log error will be copied to.
** @return void
*/
static void unqlite_logerror(unqlite *conn, const char *zBuf) {
    int iLen;
    /* Something goes wrong, extract database error log */
    unqlite_config(conn, UNQLITE_CONFIG_ERR_LOG, &zBuf, &iLen);
#ifdef LUANOSQL_DEBUG
    if( iLen > 0 )
    {
        puts(zBuf);
    }
#endif
}

/*
** Check for valid environment.
** @param L the lua state
** @return env_data a valid env_data structure
*/
static env_data *getenvironment(lua_State *L) {
    env_data *env = (env_data *)luaL_checkudata(L, 1, LUANOSQL_ENVIRONMENT_UNQLITE);
    luaL_argcheck(L, env != NULL, 1, LUANOSQL_PREFIX"environment expected");
    luaL_argcheck(L, !env->closed, 1, LUANOSQL_PREFIX"environment is closed");
    return env;
}

/*
** Check for valid connection.
** @param L the lua state
** @return conn_data a valid conn_data structure
*/
static conn_data *getconnection(lua_State *L) {
    conn_data *conn = (conn_data *)luaL_checkudata (L, 1, LUANOSQL_CONNECTION_UNQLITE);
    luaL_argcheck(L, conn != NULL, 1, LUANOSQL_PREFIX"connection expected");
    luaL_argcheck(L, !conn->closed, 1, LUANOSQL_PREFIX"connection is closed");
    return conn;
}

/*
** Check for valid cursor.
** @param L the lua state
** @return cur_data a valid cur_data structure / cursor
*/
static cur_data *getcursor(lua_State *L) {
    cur_data *cur = (cur_data *)luaL_checkudata (L, 1, LUANOSQL_CURSOR_UNQLITE);
    luaL_argcheck(L, cur != NULL, 1, LUANOSQL_PREFIX"cursor expected");
    luaL_argcheck(L, !cur->closed, 1, LUANOSQL_PREFIX"cursor is closed");
    return cur;
}

/*
** Closes the cursor and reset all structure fields.
** @param L the lua state
** @param cur_data curor to be detroyed
** @return void
*/
static void cur_destroy(lua_State *L, cur_data *cur)
{
    conn_data *conn;

    /* Nullify structure fields. */
    cur->closed = 1;
    cur->cursor = NULL;
    lua_rawgeti (L, LUA_REGISTRYINDEX, cur->conn);
    conn = lua_touserdata (L, -1);
    conn->cur_counter--;
    luaL_unref(L, LUA_REGISTRYINDEX, cur->conn);
    luaL_unref(L, LUA_REGISTRYINDEX, cur->cur_key_cb);
    luaL_unref(L, LUA_REGISTRYINDEX, cur->cur_key_cb_udata);
    luaL_unref(L, LUA_REGISTRYINDEX, cur->cur_data_cb);
    luaL_unref(L, LUA_REGISTRYINDEX, cur->cur_data_cb_udata);
}

/*
** Create a new Connection object and push it on top of the stack.
** @param L the lua state
** @param env the integer reference to the env
** @param unqlite_conn unqlite connection object
** @return conn_data a valid conn_data structure
*/
static int create_connection(lua_State *L, int env, unqlite *unqlite_conn)
{
    conn_data *conn = (conn_data*)lua_newuserdata(L, sizeof(conn_data));
    luanosql_setmeta(L, LUANOSQL_CONNECTION_UNQLITE);

    /* Initialize data structure */
    conn->closed = 0;
    conn->env = LUA_NOREF;
    conn->unqlite_conn = unqlite_conn;
    conn->cur_counter = 0;
    conn->con_fetch_cb =
        conn->con_fetch_cb_udata = LUA_NOREF;
    conn->L = L;
    lua_pushvalue (L, env);
    conn->env = luaL_ref (L, LUA_REGISTRYINDEX);

    return 1;
}


/**
**  These are cursor function
*/

/*
** Create a new Cursor object and push it on top of the stack.
** It wraps unqlite_kv_cursor_init.
** int unqlite_kv_cursor_init(unqlite *pDb,unqlite_kv_cursor **ppOut);
** @param L the lua state
** @return integer 1 or luanosql_faildirect with err msg
*/
static int conn_create_cursor(lua_State *L)
{
    conn_data *conn = getconnection(L);
    int res;
    const char *errmsg;
    unqlite_kv_cursor *ucursor;
    /* init a cursor for this connection */
    res = unqlite_kv_cursor_init(conn->unqlite_conn,&ucursor);
    if (res != UNQLITE_OK) {
        unqlite_logerror(conn->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }
    /* Create our own cursor internal structure */
    cur_data *cur = (cur_data*)lua_newuserdata(L, sizeof(cur_data));
    luanosql_setmeta (L, LUANOSQL_CURSOR_UNQLITE);

    /* increment cursor count this connection */
    conn->cur_counter++;
    /* fill in cur structure */
    cur->cursor = ucursor;
    cur->closed = 0;
    cur->conn = LUA_NOREF;
    cur->cur_key_cb =
    cur->cur_key_cb_udata =
    cur->cur_data_cb =
    cur->cur_data_cb_udata = LUA_NOREF;
    cur->conn_data = conn;
    lua_pushvalue(L, 1);
    cur->conn = luaL_ref(L, LUA_REGISTRYINDEX);
    return 1;
}

/*
** Cursor object collector function
** @param L the lua state
** @return integer 1
*/
static int cur_gc(lua_State *L)
{
    int res;
    const char *errmsg;
    cur_data *cur = (cur_data *)luaL_checkudata(L, 1, LUANOSQL_CURSOR_UNQLITE);
    if (cur != NULL && !(cur->closed))
    {
        res = unqlite_kv_cursor_release(cur->conn_data->unqlite_conn, cur->cursor);
        if (res != UNQLITE_OK) {
            unqlite_logerror(cur->conn_data->unqlite_conn, errmsg);
            return luanosql_faildirect(L, errmsg);
        }
        cur_destroy(L, cur);
    }
    return 0;
}

/*
** Release the cursor on top of the stack.
** Release a cursor object (deallocation) 
** It wraps unqlite_kv_cursor_release.
** int unqlite_kv_cursor_release(unqlite *pDb,unqlite_kv_cursor *pCur);
** @param L the lua state 
** @return integer 1 or luanosql_faildirect
** 
*/
static int cur_release(lua_State *L)
{
    int res;
    const char *errmsg;
    cur_data *cur = (cur_data *)luaL_checkudata(L, 1, LUANOSQL_CURSOR_UNQLITE);
    luaL_argcheck(L, cur != NULL, 1, LUANOSQL_PREFIX"cursor expected");
    if (cur->closed) {
        lua_pushboolean(L, 0);
        return 1;
    }
    res = unqlite_kv_cursor_release(cur->conn_data->unqlite_conn, cur->cursor);
    if (res != UNQLITE_OK) {
        unqlite_logerror(cur->conn_data->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }
    cur_destroy(L, cur);
    lua_pushboolean(L, 1);
    return 1;
}

/*
** Move a cursor to seek a record
** It wraps unqlite_kv_cursor_first_entry.
** int unqlite_kv_cursor_seek(unqlite_kv_cursor *pCursor,const void *pKey,int nKeyLen,int iPos);
** iPos param can one among: UNQLITE_CURSOR_MATCH_EXACT or 0 (default), UNQLITE_CURSOR_MATCH_LE or
** UNQLITE_CURSOR_MATCH_GE (see unqlite documentation).
** @see luanosql_faildirect
** Seek with the cursor
** @param L the lua state 
** @return integer 1 or luanosql_faildirect
*/
static int cur_seek(lua_State *L)
{
    int res;
    const char *errmsg;
    size_t iLen;
    cur_data *cur = getcursor(L);
    const char *key = luaL_checklstring(L, 2, &iLen);
    // fallback in default
    if (lua_gettop(L) < 3 || lua_isnil(L, 3) || luaL_checkint(L,3) > 2 /* possible values 0,1,2 */)
    {
        res = unqlite_kv_cursor_seek(cur->cursor, key, iLen, UNQLITE_CURSOR_MATCH_EXACT);
        if (res == UNQLITE_NOTFOUND) {
            lua_pushboolean(L, 0); /* not ok, but it means not found -> we manage this case */
            return 1;
        }
        if (res != UNQLITE_OK) {
            unqlite_logerror(cur->conn_data->unqlite_conn, errmsg);
            return luanosql_faildirect(L, errmsg);
        }
    } else
    {
        res = unqlite_kv_cursor_seek(cur->cursor, (const char *)key, iLen, luaL_checkint(L,3));
        if (res != UNQLITE_OK) {
            unqlite_logerror(cur->conn_data->unqlite_conn, errmsg);
            return luanosql_faildirect(L, errmsg);
        }
    }
    lua_pushboolean(L, 1);
    return 1;
}

/*
** Move the cursor to the first entry
** It wraps unqlite_kv_cursor_first_entry.
** int unqlite_kv_cursor_first_entry(unqlite_kv_cursor *pCursor);
** @param L the lua state 
** @return integer 1 or luanosql_faildirect
*/
static int cur_first_entry(lua_State *L)
{
    int res;
    const char *errmsg;
    cur_data *cur = getcursor(L);
	
    res = unqlite_kv_cursor_first_entry(cur->cursor);
    /* check result */
	if (res != UNQLITE_OK) {
        unqlite_logerror(cur->conn_data->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }
    lua_pushboolean(L, 1);
    return 1;
}

/*
** Move the cursor to the last entry
** It wraps unqlite_kv_cursor_last_entry.
** int unqlite_kv_cursor_last_entry(unqlite_kv_cursor *pCursor);
** @param L the lua state 
** @return integer 1 or luanosql_faildirect
*/
static int cur_last_entry(lua_State *L)
{
    int res;
    const char *errmsg;
    cur_data *cur = getcursor(L);
    res = unqlite_kv_cursor_last_entry(cur->cursor);
    if (res != UNQLITE_OK) {
        unqlite_logerror(cur->conn_data->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }
    lua_pushboolean(L,1);
    return 1;
}

/*
** Check cursor validity
** It wraps unqlite_kv_cursor_valid_entry.
** int unqlite_kv_cursor_valid_entry(unqlite_kv_cursor *pCursor);
** @param L the lua state 
** @return integer 1 (true or false pushed on lua stack)
*/
static int cur_is_valid_entry(lua_State *L)
{
    int res;
    const char *errmsg;
    cur_data *cur = getcursor(L);
    res = unqlite_kv_cursor_valid_entry(cur->cursor);
    lua_pushboolean(L, res);
    return 1;
}

/*
** Set cursor pointing to previous entry
** It wraps unqlite_kv_cursor_prev_entry.
** int unqlite_kv_cursor_prev_entry(unqlite_kv_cursor *pCursor);
** @param L the lua state 
** @return integer 1 or luanosql_faildirect
*/
static int cur_prev_entry(lua_State *L)
{
    int res;
    const char *errmsg;
    cur_data *cur = getcursor(L);
    res = unqlite_kv_cursor_prev_entry(cur->cursor);
	if (res != UNQLITE_OK) {
        unqlite_logerror(cur->conn_data->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }
    lua_pushboolean(L, 1);
    return 1;
}

/*
** Set cursor pointing to next entry
** It wraps unqlite_kv_cursor_next_entry.
** int unqlite_kv_cursor_next_entry(unqlite_kv_cursor *pCursor);
** @param L the lua state 
** @return integer 1 or luanosql_faildirect
*/
static int cur_next_entry(lua_State *L)
{
    int res;
    const char *errmsg;
    cur_data *cur = getcursor(L);
    res = unqlite_kv_cursor_next_entry(cur->cursor);
	if (res != UNQLITE_OK) {
        unqlite_logerror(cur->conn_data->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }
    lua_pushboolean(L, 1);
    return 1;
}

/*
** Delete an entry using cursor
** It wraps unqlite_kv_cursor_delete_entry.
** int unqlite_kv_cursor_delete_entry(unqlite_kv_cursor *pCursor);
** @param L the lua state 
** @return integer 1 or luanosql_faildirect
*/
static int cur_delete_entry(lua_State *L)
{
    int res;
    const char *errmsg;
    cur_data *cur = getcursor(L);
    res = unqlite_kv_cursor_delete_entry(cur->cursor);
    if (res != UNQLITE_OK) {
        unqlite_logerror(cur->conn_data->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }
	lua_pushboolean(L, 1);
    return 1;
}

/*
** Use a cusor to get a key
** It wraps unqlite_kv_cursor_key.
** int unqlite_kv_cursor_key(unqlite_kv_cursor *pCursor,void *pBuf,int *pnByte);
** @param L the lua state 
** @return integer 1 or luanosql_faildirect
*/
static int cur_get_key(lua_State *L)
{
    int res;
    const char *errmsg;
    char *buf;
    int bufLen = 0;
    cur_data *cur = getcursor(L);

    /* Get length first */
    res = unqlite_kv_cursor_key(cur->cursor, NULL, &bufLen);
    if (res != UNQLITE_OK) {
        unqlite_logerror(cur->conn_data->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }

    /* Allocating buffer */
    buf = (char *)malloc(bufLen);

    if( buf == NULL ) {
        luaL_error(L, LUANOSQL_PREFIX"Cannot allocate buffer");
    }

    /* It's data time! */
    res = unqlite_kv_cursor_key(cur->cursor, buf, &bufLen);
    if (res != UNQLITE_OK) {
        free(buf);
        unqlite_logerror(cur->conn_data->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }
    lua_pushlstring(L, buf, (size_t)bufLen);
    free(buf);
    return 1;
}

/*
** Use a cursor to get a data. Data length is firstly retrieved, then another call 
** to get real data.This could not seem efficient but it is helpful for large data.
** It wraps unqlite_kv_cursor_data.
** int unqlite_kv_cursor_data(unqlite_kv_cursor *pCursor,void *pBuf,unqlite_int64 *pnData);
** @param L the lua state 
** @return integer 1 or luanosql_faildirect
*/
static int cur_get_data(lua_State *L)
{
    int res;
    const char *errmsg;
    char *buf;
    unqlite_int64 bufLen;
    cur_data *cur = getcursor(L);
    /* Get length first */
    res = unqlite_kv_cursor_data(cur->cursor, NULL, &bufLen);
    if (res != UNQLITE_OK) {
        unqlite_logerror(cur->conn_data->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }

    /* Allocating buffer */
    buf = (char *)malloc(bufLen);

    if( buf == NULL ) {
        luaL_error(L, LUANOSQL_PREFIX"Cannot allocate buffer");
    }

    /* It's data time! */
    res = unqlite_kv_cursor_data(cur->cursor, buf, &bufLen);
    if (res != UNQLITE_OK) {
        free(buf);
        unqlite_logerror(cur->conn_data->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }
    /* FIXME: works in major cases (up to u32, here unqlite_int64),
    size_t here could truncate. Check size_t doc */
    lua_pushlstring(L, buf, (size_t)bufLen);
    free(buf);
    return 1;
}

/**
**  These are connection function
*/

/*
** Connection object collector function
** This is garbarge collection function  for data related to  a
** connection object.
** @param L the lua state 
** @return integer 1 or luanosql_faildirect
*/
static int conn_gc(lua_State *L)
{
    conn_data *conn = (conn_data *)luaL_checkudata(L, 1, LUANOSQL_CONNECTION_UNQLITE);
    if (conn != NULL && !(conn->closed))
    {
        if (conn->cur_counter > 0)
            return luaL_error (L, LUANOSQL_PREFIX"there are open cursors");

        /* Nullify structure fields. */
        conn->closed = 1;
        luaL_unref(L, LUA_REGISTRYINDEX, conn->env);
        luaL_unref(L, LUA_REGISTRYINDEX, conn->con_fetch_cb);
        luaL_unref(L, LUA_REGISTRYINDEX, conn->con_fetch_cb_udata);
        unqlite_close(conn->unqlite_conn);
        
    }
    return 0;
}


/*
** Close a Connection object.
** @param L the lua state 
** @return integer 1
*/
static int conn_close(lua_State *L)
{
    conn_data *conn = (conn_data *)luaL_checkudata(L, 1, LUANOSQL_CONNECTION_UNQLITE);
    luaL_argcheck (L, conn != NULL, 1, LUANOSQL_PREFIX"connection expected");
    if (conn->closed)
    {
        lua_pushboolean(L, 0);
        return 1;
    }
    /* Clean up */
    conn_gc(L);
    lua_pushboolean(L, 1);
    return 1;
}



/*
** Commit the current transaction.
** This is normally not needed as closing connection allows unqlite
** automatically commit all operations such as kvfetch,kvdelete, kvappend.
** It wraps unqlite_commit.
** int unqlite_commit(unqlite *pDb);
** @param L the lua state 
** @return integer 1 or luanosql_faildirect
*/
static int conn_commit(lua_State *L)
{
    conn_data *conn = getconnection(L);
    int res;

    res = unqlite_commit(conn->unqlite_conn);

    if (res != UNQLITE_OK)
    {
        lua_pushnil(L);
        lua_pushliteral(L, LUANOSQL_PREFIX);
        lua_concat(L, 2);
        return 2;
    }
    lua_pushboolean(L, 1);
    return 1;
}


/*
** Rollback the current transaction.
** It wraps unqlite_commit.
** int unqlite_commit(unqlite *pDb);
** @param L the lua state 
** @return integer 1
*/
static int conn_rollback(lua_State *L)
{
    conn_data *conn = getconnection(L);
    int res;

    res = unqlite_rollback(conn->unqlite_conn);
    if( res!= UNQLITE_OK)
    {
        lua_pushnil(L);
        lua_pushliteral(L, LUANOSQL_PREFIX);
        lua_concat(L, 2);
        return 2;
    }
    lua_pushboolean(L, 1);
    return 1;
}


/*
** unqlite_kv_fetch_callback callback:
** Params: database, callback function, userdata
**
*** fetch_callback:
** Params: pData,iDataLen, pUserData 
** @param pData void* output provided 
** @param iDataLen output length integer  
** @param pUserData passed is a conn_data
** @return integer 0
*/
static int fetch_callback(const void *pData, unsigned int iDataLen, void *pUserData /* conn_data for us */) {
    conn_data *conn = (conn_data*)pUserData;
    int res = 0;
    lua_State *L = conn->L;
    int top = lua_gettop(L);
    /* setup lua callback */
    lua_rawgeti(L, LUA_REGISTRYINDEX, conn->con_fetch_cb);    /* get the callback function */
    /* pData push  param */
    lua_pushlstring(L, pData, (size_t)iDataLen);
    /* iDataLen push  param */
    lua_pushinteger(L, iDataLen);
    /* get callback user data */
    lua_rawgeti(L, LUA_REGISTRYINDEX, conn->con_fetch_cb_udata);
    /* call lua function */
    res = lua_pcall(L, 3, 1, 0);
    lua_settop(L, top);
    return 0;
}


/**
** Expecting a Lua function looking like this:
** kvfetch_callback(key, mycallback, udata or nil)
** mycallback(pout, udata)
** (pUserData passed is a conn_data struct)
** It unqlite_kv_fetch_callback.
** int unqlite_kv_fetch_callback(unqlite *pDb, const void *pKey,int nKeyLen,
**    int (*xConsumer)(const void *pData,unsigned int iDataLen,void *pUserData),
**    void *pUserData);
** @param L the lua state 
** @return integer 0
*/
static int conn_kv_fetch_callback(lua_State *L) {
    size_t iLen;
    conn_data *conn = getconnection(L);
    const char *key = luaL_checklstring(L, 2, &iLen);

    if (lua_gettop(L) < 3 || lua_isnil(L, 3)) {
        luaL_unref(L, LUA_REGISTRYINDEX, conn->con_fetch_cb);
        luaL_unref(L, LUA_REGISTRYINDEX, conn->con_fetch_cb_udata);

        conn->con_fetch_cb = LUA_NOREF;
        conn->con_fetch_cb_udata = LUA_NOREF;

        /* clear con_fetch_cb handler */
        unqlite_kv_fetch_callback(conn->unqlite_conn, key, iLen, NULL,NULL);
    }
    else {
        luaL_checktype(L, 3, LUA_TFUNCTION);
        /* always userdata field (even if nil) */
        lua_settop(L, 4);

        luaL_unref(L, LUA_REGISTRYINDEX, conn->con_fetch_cb);
        luaL_unref(L, LUA_REGISTRYINDEX, conn->con_fetch_cb_udata);
        conn->con_fetch_cb_udata = luaL_ref(L, LUA_REGISTRYINDEX);
        conn->con_fetch_cb = luaL_ref(L, LUA_REGISTRYINDEX);

        /* set kv_fetch_callback handler */
        unqlite_kv_fetch_callback(conn->unqlite_conn, key, iLen, fetch_callback, conn);
    }
    return 0;
}


/*
** Store key and data
** wraps unqlite_kv_store to a data source.
** int unqlite_kv_store(unqlite *pDb,const void *pKey,int nKeyLen,const void *pData,unqlite_int64 nDataLen);
** @param L the lua state 
** @return integer 0 or 2 with luanosql_faildirect
*/
static int conn_kv_store(lua_State *L)
{
    const char *errmsg;
    int res;
    size_t iKeyLen, iDataLen;
    conn_data *conn = getconnection(L);
    const char *key = luaL_checklstring(L, 2, &iKeyLen);
    const char *data = luaL_checklstring(L, 3, &iDataLen);
    /* assuming */
    res = unqlite_kv_store(conn->unqlite_conn, key, iKeyLen, data, iDataLen);

    if (res != UNQLITE_OK)
    {
        unqlite_logerror(conn->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }
    lua_pushboolean(L, 1);
    return 1;
}


/*
** Append key / data to an existing record. If record does not exist it is created.
** wraps unqlite_kv_append to a data source.
** int unqlite_kv_append(unqlite *pDb,const void *pKey,int nKeyLen,const void *pData,unqlite_int64 nDataLen);
** @param L the lua state 
** @return integer 1 if ok, 2 for luanosql_faildirect(L, errmsg);
*/
static int conn_kv_append(lua_State *L)
{
    const char *errmsg;
    int res;
    size_t iKeyLen,iDataLen;
    conn_data *conn = getconnection(L);
    const char *key = luaL_checklstring(L, 2, &iKeyLen);
    const char *data = luaL_checklstring(L,3, &iDataLen);

    res = unqlite_kv_append(conn->unqlite_conn, key, iKeyLen, data, iDataLen);

    if (res != UNQLITE_OK)
    {
        unqlite_logerror(conn->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }
    lua_pushboolean(L, 1);
    return 1;
}




/*
** Fetch data for a given key
** wraps unqlite_kv_fetch to a data source.
** int unqlite_kv_fetch(unqlite *pDb,const void *pKey,int nKeyLen,void *pBuf,unqlite_int64 pBufLen);
** @param L the lua state 
** @return integer 1 if ok, 2 for luanosql_faildirect(L, errmsg);
*/
static int conn_kv_fetch(lua_State *L)
{
    const char *errmsg;
    int res;
    size_t iLen;
    unqlite_int64 nBytes = 0;  //Data length
    conn_data *conn = getconnection(L);
    const char *key = luaL_checklstring(L, 2, &iLen);
    char *zBuf;     //Dynamically allocated buffer

    // Get the length first, later get data. Can we do it in one-shot?
    res = unqlite_kv_fetch(conn->unqlite_conn, key, iLen, NULL, &nBytes);
	
	if (res == UNQLITE_NOTFOUND) {
        lua_pushboolean(L, 1);
        lua_pushnil(L);
        return 2;
    }
    if (res != UNQLITE_OK) {
        unqlite_logerror(conn->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }

    //Allocate a buffer big enough to hold the record content
    zBuf = (char *)malloc(nBytes);
    // TODO: why cannot we use an assert here?
    if( zBuf == NULL ) {
        return;
    } 

    //Copy record content in our buffer now
    res = unqlite_kv_fetch(conn->unqlite_conn, key, iLen,zBuf, &nBytes);

    // TODO: refactor, this check is done 1000 times.
    if (res != UNQLITE_OK)
    {
        unqlite_logerror(conn->unqlite_conn, errmsg);
		free(zBuf);
        return luanosql_faildirect(L, errmsg);
    }
    lua_pushboolean(L, 1);
    lua_pushlstring(L, zBuf, nBytes);
    free(zBuf);
    return 2;
}


/*
** Delete a record passing the key. if the record is not found it returns
** true anyway as deleting a non existent record does not have any side-effect.
** wraps unqlite_kv_delete to a data source.
** int unqlite_kv_delete(unqlite *pDb,const void *pKey,int nKeyLen);
** @param L the lua state 
** @return integer 1 if ok, 2 for luanosql_faildirect(L, errmsg);
*/
static int conn_kv_delete(lua_State *L)
{
    const char *errmsg;
    int res;
    size_t iLen;
    conn_data *conn = getconnection(L);
    const char *key = luaL_checklstring(L, 2, &iLen);

    res = unqlite_kv_delete(conn->unqlite_conn, key, iLen);

    if (res != UNQLITE_OK && res != UNQLITE_NOTFOUND)
    {
        unqlite_logerror(conn->unqlite_conn, errmsg);
        return luanosql_faildirect(L, errmsg);
    }
    lua_pushboolean(L, 1);
    return 1;
}


/*
** This section is for environment object functions.
*/

/*
** Environment object collector function.
** @param L the lua state 
** @return integer 0
*/
static int env_gc (lua_State *L)
{
    env_data *env = (env_data *)luaL_checkudata(L, 1, LUANOSQL_ENVIRONMENT_UNQLITE);
    if (env != NULL && !(env->closed))
        env->closed = 1;
    return 0;
}


/*
** Close environment object.
** @param L the lua state 
** @return integer 1 if ok
*/
static int env_close (lua_State *L)
{
    env_data *env = (env_data *)luaL_checkudata(L, 1, LUANOSQL_ENVIRONMENT_UNQLITE);
    luaL_argcheck(L, env != NULL, 1, LUANOSQL_PREFIX"environment expected");
    if (env->closed) {
        lua_pushboolean(L, 0);
        return 1;
    }
    env_gc(L);
    lua_pushboolean(L, 1);
    return 1;
}


/*
** Open a connection with DB (it is always created in UNQLITE_OPEN_READWRITE, UNQLITE_OPEN_CREATE)
** Connects to a data source.
** @param L the lua state 
** @return integer 1 if ok, 2 for luanosql_faildirect(L, errmsg);
*/
static int env_connect(lua_State *L)
{
    const char *sourcename;
    unqlite *conn;
    const char *errmsg;
    int res;
    getenvironment(L);  /* validate environment */

    sourcename = luaL_checkstring(L, 2);
    res = unqlite_open(&conn, sourcename, UNQLITE_OPEN_READWRITE | UNQLITE_OPEN_CREATE);

    if (res != UNQLITE_OK)
    {
        unqlite_logerror(conn, errmsg);
		unqlite_close(conn);
        return luanosql_faildirect(L, errmsg);
    }
    return create_connection(L, 1, conn);
}

/*
** Create metatables for each class of object.
** @param L the lua state 
** @return void
*/
static void create_metatables (lua_State *L)
{
    struct luaL_Reg environment_methods[] = {
        {"__gc", env_gc},
        {"close", env_close},
        {"connect", env_connect},
        {NULL, NULL},
    };
    struct luaL_Reg connection_methods[] = {
        {"__gc", conn_gc},
        {"close", conn_close},
        {"commit", conn_commit},
        {"rollback", conn_rollback},
        {"kvstore", conn_kv_store},
        {"kvappend", conn_kv_append},
        {"kvfetch", conn_kv_fetch},
        {"kvdelete", conn_kv_delete},
        {"kvfetch_callback", conn_kv_fetch_callback},
        {"create_cursor", conn_create_cursor},
        {NULL, NULL},
    };
    struct luaL_Reg cursor_methods[] = {
        {"__gc", cur_gc},
        {"release", cur_release},
        {"seek", cur_seek},
        {"first_entry", cur_first_entry},
        {"last_entry", cur_last_entry},
        {"is_valid_entry", cur_is_valid_entry},
        {"prev_entry", cur_prev_entry},
        {"next_entry", cur_next_entry},
        {"cursor_key", cur_get_key},
        {"cursor_data", cur_get_data},
        {"delete_entry", cur_delete_entry},
        //{"key_callback", cur_key_callback},   /** to be implemented */
        //{"data_callback", cur_data_callback}, /** to be implemented */
        {NULL, NULL},
    };
    luanosql_createmeta(L, LUANOSQL_ENVIRONMENT_UNQLITE, environment_methods);
    luanosql_createmeta(L, LUANOSQL_CONNECTION_UNQLITE, connection_methods);
    luanosql_createmeta(L, LUANOSQL_CURSOR_UNQLITE, cursor_methods);
    lua_pop (L, 3);
}


/*
** Creates an Environment and returns it.
** @param L the lua state 
** @return integer 1
*/
static int create_environment (lua_State *L)
{
    env_data *env = (env_data *)lua_newuserdata(L, sizeof(env_data));
    luanosql_setmeta(L, LUANOSQL_ENVIRONMENT_UNQLITE);

    /* fill in structure */
    env->closed = 0;
    return 1;
}


/*
** Creates the metatables for the objects and registers the
** driver open method.
** @param L the lua state 
** @return integer 1
*/
LUANOSQL_API int luaopen_luanosql_unqlite(lua_State *L)
{
    struct luaL_Reg driver[] = {
        {"unqlite", create_environment},
        {NULL, NULL},
    };
    create_metatables (L);
    lua_newtable (L);
    luaL_setfuncs (L, driver, 0);
    luanosql_set_info (L);
    return 1;
}
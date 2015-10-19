#!/usr/bin/env lua

----------------------------------------------------------------------------
-- These tests use lua telescope (https://github.com/norman/telescope)
-- Thanks to Norman Clarke for this great testing framework 
----------------------------------------------------------------------------

-- Here some require, we do assertion anyway
require"string"
require"os"
local driver = require"luanosql.unqlite"



-- simple table to be inserted in DB
local mlist = {["key1"]="value-1", ["key2"]="value-2", ["key3"]="value-3", 
  ["key4"]="value-4", ["key5"]="value-5", ["key6"]="value-6", 
  ["key7"]="value-7", ["key8"]="value-8", ["key9"]="value-9", 
  ["key10"]="value-10"}


-- Utility function for sorting the table
local function pairsByKeys (t, f)
    local a = {}
    for n in pairs(t) do table.insert(a, n) end
    table.sort(a, f)
    local i = 0      -- iterator variable
    local iter = function ()   -- iterator function
        i = i + 1
        if a[i] == nil then return nil
        else return a[i], t[a[i]]
        end
    end
    return iter
end



-- define a custom callback for fetch
local function custom_test_fetch_callback(dataout,datalen,ud)
  --print("UnQLite Update Hook:",dataout,datalen,ud)
  local t1,t2,t3 = ud["1"], ud["2"],ud["3"]
  --print("Userdata:", t1,t2,t3)
  -- check with a binary object passed, we use conection itself
  local r,v = t3:kvfetch("key4")
  -- no assert here, this is a callback and errors are NOT managed (at C-level)
  if r and v=="value-4" then 
	print("Lua Callback key OK")
  end
end



  
-- In this context we address data manipulation on an already opened connection
-- such as put/get records with data, append and delete.
context("User should be able to create/close a connection", function()
	
	-- connection to db
	local conn, env
	
	-- create an environment
	test("Should be able to create unqlite environment", function ()
		env = assert(driver.unqlite())
		assert_not_nil(env)
	end)
	
	-- create a connection
	test("Should be able to create a connection passing dbname", function ()
		conn = assert(env:connect("lns-unqlite.testdb"))
		assert_not_nil(conn)
	end)
	
	-- nested context, manage data
	context("User should be able to manage (store/fetch/delete data)", function()
			
		test("Should be able to insert one simple string", function ()
			local res, err = conn:kvstore("Hello","World!")
			assert_true(res and err==nil)
			
		end)
		
		test("Should be able to fetch one simple string", function ()
			local res, data = conn:kvfetch("Hello")
			assert_true(res and data == "World!")
		end)
		
		test("Should be able to delete an existing simple strings", function ()
			local res, err = conn:kvdelete("Hello")
			assert_true(res and err==nil)
		end)
		
		test("Should be able to (try to) fetch a non-existing record", function ()
			local res, data = conn:kvfetch("Hello")
			assert_true(res and data==nil)
		end)
		
		test("Should be able to (try to) delete a non-existent record", function ()
			-- it should not be there anymore, already deleted
			-- this check also if 'real' delete is OK
			local res, merror = conn:kvdelete("Hello")
			assert_true(res and merror==nil)
			
		end)
		
		test("Should be able to store a bunch of strings", function ()
			for k, v in pairsByKeys(mlist) do
				local res, err = conn:kvstore(k,v)
				assert_true(res and err==nil)
			end
		end)
		
		test("Should be able to retrieve a bunch of strings", function ()
			for k, v in pairsByKeys(mlist) do
				local res, data = conn:kvfetch(k)
				assert_equal(v,data)
			end
		end)
		
		test("Should be able to append an existing string record", function ()
			local res, err = conn:kvappend("key10","-appended")
			assert_true(res and err==nil)
			-- check that it has been appended
			local res2, data2 = conn:kvfetch("key10")
			assert_true(res2) 
			assert_not_nil(data2)
			-- real check
			assert_equal(data2, "value-10-appended")
		end)
		
		test("Should be able to register a consumer callback to redirect data retrieval", function()
			--  Try to pass userdata which can be used by callback itself
			--print("Check Callback on fetch")
			-- passing a mix of objects and strings to verify we can use objects also
			local tbl = {["1"]="Lippa",["2"]="Cippa",["3"]=conn}
			conn:kvfetch_callback("key9",custom_test_fetch_callback, tbl)
			--print("Callback registered")
			-- kvfetch a value
			res, myval = conn:kvfetch("key9")
			assert_true(res and myval == "value-9")
		end)
	
	end)
	
	-- close connection
	test("Should be able to close the connection (autocommit)", function ()
		assert_true(conn:close())
	end)
	
	-- destroy the connection even if already closed (it should return false)
	test("Should NOT be able to close an already closed connection", function ()
		assert_false(conn:close())
	end)
	
	-- check data persistence re-opening the connection, fetch and check data
	test ("Should be able to check data have been written", function()
		local conn = assert(env:connect("lns-unqlite.testdb"))
		assert_not_nil(conn)
		for k,v in pairsByKeys(mlist) do
			local r, d = conn:kvfetch(k)
			assert_true(r) -- result
			assert_not_nil(d) -- existence
			-- special case for appended record
			if k=="key10" then
			    assert_equal(d, "value-10-appended")
			else -- all others
			    assert_equal(d,v)
			end
		end
		-- close connection
		assert_true(conn:close())
	end)
	
	-- close the environment
	test("Should be able to close unqlite environment", function ()
		assert_true(env:close())
	end)
	
	-- destroy the environment even if already closed (it should return false)
	test("Should NOT be able to close an already closed environment", function ()
		assert_false(env:close())
	end)
	
end)




-- In this context we cover commit and rollback
context("User should be able to manually manage transactions", function()
	
	-- connection to db
	local conn, env
	
	
	-- create an environment
	test("Should be able to create unqlite environment", function ()
		-- delete db file first, we start from scratch
		os.remove("lns-unqlite.testdb")
		env = assert(driver.unqlite())
		assert_not_nil(env)
	end)
	
	-- create a connection
	test("Should be able to create a connection passing dbname", function ()
			conn = assert(env:connect("lns-unqlite.testdb"))
			assert_not_nil(conn)
	end)
	-- nested context also here
	context("User should be able commit/rollback manually a transation", function()
		
		test("Should be able to rollback a transaction", function ()
			-- insert a record
			local res, err = conn:kvstore("Mykey1","MyKeyValue1")
			assert(res and err==nil)
			-- is record there? It should be now
			local r0, d0 = conn:kvfetch("Mykey1")
			assert_true(r0)
			assert_equal(d0,"MyKeyValue1")
			-- try to rollback this
			local res2, err2 = conn:rollback()
			assert(res2 and err2==nil)
			-- check if rollback works, record should not be there
			local r, d = conn:kvfetch("Mykey1")
			assert_true(r)
			assert_nil(d)
		end)
		
		-- commit  a transaction and verify it using rollback
		test("Should be able to commit a transaction when needed", function ()
			-- insert a record
			local res, err = conn:kvstore("MySecondkey1","MySecondKeyValue1")
			assert(res and err==nil)
			local res1, err1 = conn:kvstore("MySecondkey2","MySecondKeyValue2")
			assert(res1 and err1==nil)
			-- try to commit these two
			local res2, err2 = conn:commit()
			assert(res2 and err2==nil)
			-- store another one
			local res3, err3 = conn:kvstore("MyThirdkey1","MyThirdKeyValue1")
			assert(res3 and err3==nil)
			-- try to rollback this
			local r, e = conn:rollback()
			assert(r and e==nil)
			-- check if rollback works, last record should not be there
			local r1, d1 = conn:kvfetch("MyThirdkey1")
			assert_true(r1)
			assert_nil(d1)
			-- now check that the first two are still there
			local r2, d2 = conn:kvfetch("MySecondkey1")
			assert_true(r2)
			assert_equal(d2,"MySecondKeyValue1")
			local r3, d3 = conn:kvfetch("MySecondkey2")
			assert_true(r3)
			assert_equal(d3,"MySecondKeyValue2")
		end)
	end)
	
	-- close connection
	test("Should be able to close the connection", function ()
		assert_true(conn:close())
	end)
	
	-- close the environment
	test("Should be able to close unqlite environment", function ()
		assert_true(env:close())
	end)
	
	
end) -- end context


-- In this context we address unqlite cursors functions
context("User should be able to manage data using cursor", function()
	
	-- connection to db
	local conn, env
	
	
	-- create an environment
	test("Should be able to create unqlite environment", function ()
		-- delete db file first, we start from scratch
		assert(os.remove("lns-unqlite.testdb"))
		env = assert(driver.unqlite())
		assert_not_nil(env)
	end)
	
	-- create a connection
	test("Should be able to create a connection passing dbname", function ()
		conn = assert(env:connect("lns-unqlite.testdb"))
		assert_not_nil(conn)
	end)
	
	
	test("Should be able to store a bunch of strings", function ()
		for k, v in pairsByKeys(mlist) do
			local res, err = conn:kvstore(k,v)
			assert_true(res and err==nil)
		end
		-- commit transaction, force
		assert_true(conn:commit())
	end)
	
	-- nested context also here
	context("User should be able to use cursors", function()
		
		-- cursor
		local cur
		
		test("Should be able to create a cursor", function ()
			-- create a cursor
			cur = conn:create_cursor()
			assert_not_nil(cur)
		end)
		
		test("Should be able to create more than one cursor", function ()
			-- create 3 cursor and release them
			local cur1 = conn:create_cursor()
			assert_not_nil(cur1)
			local cur2 = conn:create_cursor()
			assert_not_nil(cur2)
			local cur3 = conn:create_cursor()
			assert_not_nil(cur3)
			assert_true(cur1:release())
			assert_true(cur2:release())
			assert_true(cur3:release())
		end)
		
		test("Should be able to move cursor to first entry", function ()
			-- insert a record
			local res, err = cur:first_entry()
			assert_true(res and err==nil)
			assert_equal(cur:cursor_data(),mlist["key1"])
			assert_equal(cur:cursor_key(), "key1")				
		end)
		
		test("Should be able to seek for an entry using cursor EXACT_MATCH", function ()
			-- search for a record
			local res, err = cur:seek("key5", 0)
			assert_true(res and err==nil)
			assert_equal(cur:cursor_data(),mlist["key5"])
			-- pass a out-of-range value, fallback to EXTACT_MATCH
			local res1, err1 = cur:seek("key4", 5)
			assert_true(res1 and err1==nil)
			assert_equal(cur:cursor_data(),mlist["key4"])
			-- avoid passing search method (nil), fallback to EXACT_MATCH
			local res2, err2 = cur:seek("key6")
			assert_true(res2 and err2==nil)
			assert_equal(cur:cursor_data(),mlist["key6"])
		end)
		
		-- test("Should be able to move cursor to seek for an entry using GE_MATCH", function ()
			-- search for a record
			-- local res, err = cur:seek("key-5", 1)
			-- assert_true(res and err==nil)
		-- end)
		
		-- test("Should be able to move cursor to seek for an entry using LE_MATCH", function ()
			-- search for a record
			-- local res, err = cur:seek("key-5", 2)
			-- assert_true(res and err==nil)
		-- end)
		
		
		test("Should be able to check if cursor is pointing to a valid entry", function ()
			local r, e = cur:seek("key2")
			assert_true(r and e==nil)
			-- check if an entry is valid
			local res = cur:is_valid_entry()
			--print("Risultato: ", res)
			assert_true(res)
			local res1 , err1 = cur:seek("key4")
			assert_true(res1 and err1==nil)
			assert_equal(cur:cursor_data(),mlist["key4"])				
		end)
		
		
		test("Should be able to move cursor to previous entry", function ()
			-- prev entry
			local res, err = cur:prev_entry()
			assert_true(res and err==nil)
			assert_equal(cur:cursor_data(),mlist["key3"])
			assert_equal(cur:cursor_key(),"key3")
		end)
		
		test("Should be able to move cursor to next entry", function ()
			-- next entry
			local res, err = cur:next_entry()
			assert_true(res and err==nil)
			assert_equal(cur:cursor_data(),mlist["key4"])
			assert_equal(cur:cursor_key(),"key4")
		end)
		
		test("Should be able to move cursor to last entry", function ()
			-- last entry
			local res, err = cur:last_entry()
			assert_true(res and err==nil)
			assert_equal(cur:cursor_data(), mlist["key9"])
			local k1 = cur:cursor_key()
			assert_equal(k1, "key9")
		end)
		
		test("Should be able to delete an entry using cursor ", function ()
			-- moving to a en entry
			local res, err = cur:seek("key7")
			assert_true(res and err==nil)
			-- delete an entry	
			local res1, err1 = cur:delete_entry()
			assert_true(res1 and err1==nil)
			-- check if correctly deleted
			local res2, data = conn:kvfetch("key7")
			assert_true(res2 and data==nil)
		end)
		
		test("Should be able to release a cursor", function ()
			-- release cursor
			local res, err = cur:release()
			assert_true(res and err==nil)
		end)
		
		test("Should NOT be able to release an already released cursor", function ()
			-- release cursor
			local res, err = cur:release()
			assert_true(res==false and err==nil)
		end)
		
	end)
	
	-- close connection
	test("Should be able to close the connection", function ()
		assert_true(conn:close())
	end)
	
	-- close the environment
	test("Should be able to close unqlite environment", function ()
		assert_true(env:close())
		assert(os.remove("lns-unqlite.testdb"))
	end)
	
end)
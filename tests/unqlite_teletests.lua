#!/usr/local/bin/lua5.1

----------------------------------------------------------------------------
-- These tests use lua telescope (https://github.com/norman/telescope)
-- Thanks to Norman Clarke for this great testing framework 
----------------------------------------------------------------------------

-- Here some require, we do assertion anyway
require"string"
require"os"
local driver = require"luanosql.unqlite"
local env = assert(driver.unqlite())


-- simple table to be inserted in DB
local mlist = {["key1"]="value-1", ["key2"]="value-2", ["key3"]="value-3", 
  ["key4"]="value-4", ["key5"]="value-5", ["key6"]="value-6", 
  ["key7"]="value-7", ["key8"]="value-8", ["key9"]="value-9", 
  ["key10"]="value-10"}
  
-- In this context we address data manipulation on an already opened connection
-- such as put/get records with data, append and delete.
context("User should be able to create/close a connection", function()
	
	-- connection to db
	local conn
	
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
			for k, v in pairs(mlist) do
				local res, err = conn:kvstore(k,v)
				assert_true(res and err==nil)
			end
		end)
		
		test("Should be able to retrieve a bunch of strings", function ()
			for k, v in pairs(mlist) do
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
		
	end)
	
	-- close connection
	test("Should be able to close the connection (autocommit)", function ()
		assert_true(conn:close())
	end)
	
	-- check data persistence re-opening the connection, fetch and check data
	test ("Should be able to check data have been written", function()
		local conn = assert(env:connect("lns-unqlite.testdb"))
		assert_not_nil(conn)
		for k,v in pairs(mlist) do
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
end)




-- In this context we cover commit and rollback
context("User should be able to manually manage transactions", function()
	
	-- connection to db
	local conn
	
	-- delete db file first, we start from scratch
	os.remove("lns-unqlite.testdb")
	
	-- create a connection
	test("Should be able to create a connection passing dbname", function ()
			conn = assert(env:connect("lns-unqlite.testdb"))
			assert_not_nil(conn)
	end)
	
	context("User should be able commit/rollback manually a transation", function()
		-- create a connection
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
	
end) -- end context

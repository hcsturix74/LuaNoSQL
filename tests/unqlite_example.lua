-- See Copyright Notice in license.html
require"string"
-- load driver
driver = require"luanosql.unqlite"

print("--------------- Running first test-----------------\n")
-- create environment object
env = assert (driver.unqlite())
-- connect to unqlite data source
con = assert (env:connect("luaunqlite-test"))
-- insert a key/value element
res = assert (con:kvstore("key1","Hello World!"))

-- add a few elements key/value
list = {["key2"]="value-1", ["key3"]="value-2", ["key4"]="value-3", 
  ["key5"]="value-4", ["key6"]="value-5", ["key7"]="value-6", 
  ["key8"]="value-7", ["key9"]="value-8", ["key10"]="value-9", 
  ["key11"]="value-11"}

-- insert dummy data
print("Start inserting data")
for k, v in pairs (list) do
  print(string.format("Inserted KEY = %s , VALUE= %s",k,v))
  res = assert (con:kvstore(k,v))
end

print("Start Reading the same data e check")
-- Reread and check inserted data 
for k, v in pairs (list) do
  res, data = con:kvfetch(k)
  print(string.format("Key/Data = %s/%s --->VALUE_EXPECTED= %s",k,data,v))
  -- check data
  assert (data,v)
  print("Data checked")
end

-- kvappend call example / test
print("Appending -value11 at key=key11 itself...")
res = assert(con:kvappend("key11","-value12"))
print("OK...Appended.Let's check")
r, val = con:kvfetch("key11")
assert(val ,"value11-value12")
print("OK...Correct, go on...")


-- kvcommit call example / test
print("Committing...")
res = assert(con:commit())
print("OK...Committed.")


-- kvfetch call example / test
print("Now fetch data")
-- fetch key3 and check
res, val = con:kvfetch("key3")
print(res, val)
assert(val,"value-2")
print("OK...Fetched.")

-- kvdelete call example / test
print("Deleting")
-- delete key2
res = con:kvdelete("key2")
print(res)
print("OK...Deleted")

-- kvcommit call example / test
print("OK...Committing deletion")
res = assert(con:commit())
print("OK...Committed.")

-- Check Delete operation
print("Check deletion")
-- kvfetch call example / test
-- check that key2 is not there
res, val = con:kvfetch("key2")
print(res, val)
assert(res and val == nil)
print("OK...Deletion checked.")

-- define a custom callback for fetch
function custom_test_fetch_callback(dataout,datalen,ud)
  print("UnQLite Update Hook:",dataout,datalen,ud)
  t1,t2,t3 = ud['1'], ud['2'],ud['3']
  print("Userdata:", t1,t2,t3)
  print("Getting key4....")
  res,val = con:kvfetch("key4")
  assert(res and val == "value-3")
  print("Lua Callback key4 OK")
  return 0
end

--  Try to pass userdata which can be used by callback itself
print("Check Callback on fetch")
local tbl = {['1']='Lippa',['2']=con, ['3']='Zippa'}
con:kvfetch_callback("key9",custom_test_fetch_callback, tbl)
print("Callback registered")

-- kvfetch a value
res, myval = con:kvfetch("key9")
assert(res and myval == "value-8")


res, myval = con:kvfetch("key9")
assert(res and myval == "value-8")
print(myval)

print("Now committing and closing")
-- commit and close
con:commit()
con:close()
env:close()

print("--------------- Running another test-----------------\n")
-- Now it's cursor time :-)
-- create environment object
ev = assert (driver.unqlite())
-- connect to unqlite data source
cn = assert (ev:connect("luaunqlite-test"))
-- overwrite a key/value element
res = assert (cn:kvstore("key1","Hello World2!"))

print("Testing Cursors.....\n")
-- create a cursor
cur = cn:create_cursor()
print("Cursor created\n")
-- set it to first entry
print("Set to the first entry\n")
c2 = cur:first_entry()
print("Start looping over records.")
repeat
	if cur:is_valid_entry() then
		print(string.format("Key / Data====>\t%s / %s\n", cur:cursor_key(),cur:cursor_data()))
	else print("Invalid Entry :)")
	end
until cur:next_entry()
print("Loop end\n")

print("Release cursor\n")
assert(cur:release())
print("Now closing")
-- commit and close
cn:close()
ev:close()
print("Test OK")



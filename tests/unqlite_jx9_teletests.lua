#!/usr/bin/env lua

----------------------------------------------------------------------------
-- These tests use lua telescope (https://github.com/norman/telescope)
-- Thanks to Norman Clarke for this great testing framework 
----------------------------------------------------------------------------

-- Here some require, we do assertion anyway
require"string"
require"os"
require"io"
local driver = require"luanosql.unqlite"



-- simple jx9 to be executed
local jx9_program = [==[ db_create('users');
    db_store('users',{ 'name' : 'Dean' , 'age' : 32 });
    db_store('users',{ 'name' : 'Jack' , 'age' : 27 });
	db_store('users',{ 'name' : 'Luke' , 'age' : 33 });
	db_store('users',{ 'name' : 'Eric' , 'age' : 55 });
	db_store('users',{ 'name' : 'John' , 'age' : 87 });
	db_store('users',{ 'name' : 'Mark' , 'age' : 12 });
    ]==]

-- not so simple jx9 program (extended)
local jx9_program_extended = [==[
    db_create('users');
	// Declare a JSON object and assign it to the variable $my_data
	$my_data = {
		 // Greeting message
		 greeting : "Hello world!\n",
		 // Dummy field
		  __id : 1,
		 // Host Operating System
		 os_name: uname(), //Built-in OS function
		 // Current date
		 date : __DATE__,
		 // Return the current time using an anonymous function
		 time : function(){ return __TIME__; }
	 };
    ]==]

local jx9_program_json = [==[ 
	// Create a dummy configuration using a JSON object
	$my_config =  {
	  bind_ip : "127.0.0.1",
	  config : "/etc/symisc/jx9.conf",
	  dbpath : "/usr/local/unqlite",
	  fork : true,
	  logappend : true,
	  logpath : "/var/log/jx9.log",
	  quiet : true,
	  port : 8080
	 };

	// Dump the JSON object
	print $my_config;

	// Write to disk
	$file = "config.json.txt";
	print "\n\n------------------\nWriting JSON object to disk file: '$file'\n";

	// Create the file
	$fp = fopen($file,'w+');
	if( !$fp )
		exit("Cannot create $file");

	// Write the JSON object
	fwrite($fp,$my_config);

	// Close the file
	fclose($fp);

	// All done
	print "$file successfully created on: "..__DATE__..' '..__TIME__;

	// ---------------------- config_reader.jx9 ----------------------------

	// Read the configuration file defined above
	$iSz = filesize($zFile); // Size of the whole file
	$fp = fopen($zFile,'r'); // Read-only mode
	if( !$fp ){
	   exit("Cannot open '$zFile'");
	}

	// Read the raw content
	$zBuf = fread($fp,$iSz);

	// decode the JSON object now
	$my_config = json_decode($zBuf);
	if( !$my_config ){
		print "JSON decoding error\n";
	}else{
	  // Dump the JSON object
	  foreach( $my_config as $key,$value ){
	   print "$key ===> $value\n";
	  }
	}

	// Close the file
	fclose($fp);

]==] 
  
-- In this context we address jx9 compile and compile file
context("User should be able to compile a jx9 program", function()
	
	-- define local variables
	local env, conn, vm  
	
	test("Should be able to create a new DB", function ()

		-- connection to db
		env  = assert(driver.unqlite())
		conn = assert(env:connect("lns-unqlite-jx9.testdb"))
		assert_not_nil(conn)
				
	end)
			
	test("Should be able to compile a jx9 program", function ()
		vm = conn:compile(jx9_program)
		assert_not_nil(vm)
	end)
	
	test("Should be able to execute a jx9 program", function ()
		local res, err = vm:vm_exec()
		assert_true(res and err==nil)
	end) 
	
	
	-- Reset a jx9 vm
	test("Should be able to reset a JX9 engine VM", function ()
		assert_true(vm:vm_reset())
	end)
	
	-- Release a jx9 vm
	test("Should be able to release a JX9 engine VM", function ()
		assert_true(vm:vm_release())
	end)
	
	-- destroy jx9 vm even if already closed (it should return false)
	test("Should NOT be able to release an already closed jx9 vm", function ()
		assert_false(vm:vm_release())
	end)
	
	-- close connection
	test("Should be able to close the connection (autocommit)", function ()
		assert_true(conn:close())
	end)
	
	-- destroy the connection even if already closed (it should return false)
	test("Should NOT be able to close an already closed connection", function ()
		assert_false(conn:close())
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
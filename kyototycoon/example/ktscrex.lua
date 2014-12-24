kt = __kyototycoon__
db = kt.db

-- log the start-up message
if kt.thid == 0 then
   kt.log("system", "the Lua script has been loaded")
end

-- echo back the input data as the output data
function echo(inmap, outmap)
   for key, value in pairs(inmap) do
      outmap[key] = value
   end
   return kt.RVSUCCESS
end

-- report the internal state of the server
function report(inmap, outmap)
   outmap["__kyototycoon__.VERSION"] = kt.VERSION
   outmap["__kyototycoon__.thid"] = kt.thid
   outmap["__kyototycoon__.db"] = tostring(kt.db)
   for i = 1, #kt.dbs do
      local key = "__kyototycoon__.dbs[" .. i .. "]"
      outmap[key] = tostring(kt.dbs[i])
   end
   local names = ""
   for name, value in pairs(kt.dbs) do
      if #names > 0 then names = names .. "," end
      names = names .. name
   end
   outmap["names"] = names
   return kt.RVSUCCESS
end

-- log a message
function log(inmap, outmap)
   local kind = inmap.kind
   local message = inmap.message
   if not message then
      return kt.RVEINVALID
   end
   if not kind then
      kind = "info"
   end
   kt.log(kind, message)
   return kt.RVSUCCESS
end

-- store a record
function set(inmap, outmap)
   local key = inmap.key
   local value = inmap.value
   if not key or not value then
      return kt.RVEINVALID
   end
   local xt = inmap.xt
   if not db:set(key, value, xt) then
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- remove a record
function remove(inmap, outmap)
   local key = inmap.key
   if not key then
      return kt.RVEINVALID
   end
   if not db:remove(key) then
      local err = db:error()
      if err:code() == kt.Error.NOREC then
         return kt.RVELOGIC
      end
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- increment the numeric string value
function increment(inmap, outmap)
   local key = inmap.key
   local num = inmap.num
   if not key or not num then
      return kt.RVEINVALID
   end
   local function visit(rkey, rvalue, rxt)
      rvalue = tonumber(rvalue)
      if not rvalue then rvalue = 0 end
      num = rvalue + num
      return num
   end
   if not db:accept(key, visit) then
      return kt.REINTERNAL
   end
   outmap.num = num
   return kt.RVSUCCESS
end

-- retrieve the value of a record
function get(inmap, outmap)
   local key = inmap.key
   if not key then
      return kt.RVEINVALID
   end
   local value, xt = db:get(key)
   if value then
      outmap.value = value
      outmap.xt = xt
   else
      local err = db:error()
      if err:code() == kt.Error.NOREC then
         return kt.RVELOGIC
      end
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- store records at once
function setbulk(inmap, outmap)
   local num = db:set_bulk(inmap)
   if num < 0 then
      return kt.RVEINTERNAL
   end
   outmap["num"] = num
   return kt.RVSUCCESS
end

-- remove records at once
function removebulk(inmap, outmap)
   local keys = {}
   for key, value in pairs(inmap) do
      table.insert(keys, key)
   end
   local num = db:remove_bulk(keys)
   if num < 0 then
      return kt.RVEINTERNAL
   end
   outmap["num"] = num
   return kt.RVSUCCESS
end

-- retrieve records at once
function getbulk(inmap, outmap)
   local keys = {}
   for key, value in pairs(inmap) do
      table.insert(keys, key)
   end
   local res = db:get_bulk(keys)
   if not res then
      return kt.RVEINTERNAL
   end
   for key, value in pairs(res) do
      outmap[key] = value
   end
   return kt.RVSUCCESS
end

-- move the value of a record to another
function move(inmap, outmap)
   local srckey = inmap.src
   local destkey = inmap.dest
   if not srckey or not destkey then
      return kt.RVEINVALID
   end
   local keys = { srckey, destkey }
   local first = true
   local srcval = nil
   local srcxt = nil
   local function visit(key, value, xt)
      if first then
         srcval = value
         srcxt = xt
         first = false
         return kt.Visitor.REMOVE
      end
      if srcval then
         return srcval, srcxt
      end
      return kt.Visitor.NOP
   end
   if not db:accept_bulk(keys, visit) then
      return kt.REINTERNAL
   end
   if not srcval then
      return kt.RVELOGIC
   end
   return kt.RVSUCCESS
end

-- list all records
function list(inmap, outmap)
   local cur = db:cursor()
   cur:jump()
   while true do
      local key, value, xt = cur:get(true)
      if not key then break end
      outmap[key] = value
   end
   return kt.RVSUCCESS
end

-- upcate all characters in the value of a record
function upcase(inmap, outmap)
   local key = inmap.key
   if not key then
      return kt.RVEINVALID
   end
   local function visit(key, value, xt)
      if not value then
         return kt.Visitor.NOP
      end
      return string.upper(value)
   end
   if not db:accept(key, visit) then
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- prolong the expiration time of a record
function survive(inmap, outmap)
   local key = inmap.key
   if not key then
      return kt.RVEINVALID
   end
   local function visit(key, value, xt)
      if not value then
         return kt.Visitor.NOP
      end
      outmap.old_xt = xt
      if xt > kt.time() + 3600 then
         return kt.Visitor.NOP
      end
      return value, 3600
   end
   if not db:accept(key, visit) then
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- count words with the MapReduce framework
function countwords(inmap, outmap)
   local function map(key, value, emit)
      local values = kt.split(value, " ")
      for i = 1, #values do
         local word = kt.regex(values[i], "[ .?!:;]", "")
         word = string.lower(word)
         if #word > 0 then
            if not emit(word, "") then
               return false
            end
         end
      end
      return true
   end
   local function reduce(key, iter)
      local count = 0
      while true do
         local value = iter()
         if not value then
            break
         end
         count = count + 1
      end
      outmap[key] = count
      return true
   end
   if not db:mapreduce(map, reduce, nil, kt.DB.XNOLOCK) then
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

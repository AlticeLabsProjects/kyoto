kt = __kyototycoon__
db = kt.db

-- prepare secondary indices
idxdbs = {}
for dbname, dbobj in pairs(kt.dbs) do
   if kt.strbwm(dbname, ".kct") then
      local prefix = kt.regex(dbname, ".*/", "")
      prefix = kt.regex(dbname, ".kct$", "")
      if #prefix > 0 then
         idxdbs[prefix] = dbobj
      end
   end
end

-- set a record
function set(inmap, outmap)
   local id = tostring(inmap.id)
   if not id then
      return kt.RVEINVALID
   end
   local err = false
   inmap.id = nil
   local serial = kt.mapdump(inmap)
   -- visitor function
   local function visit(key, value, xt)
      -- clean up indices
      if value then
         local obj = kt.mapload(value)
         for rkey, rvalue in pairs(obj) do
            local idxdb = idxdbs[rkey]
            if idxdb then
               local idxkey = rvalue .. " " .. id
               if not idxdb:remove(idxkey) then
                  kt.log("error", "removing an index entry failed")
                  err = true
               end
            end
         end
      end
      -- insert into indices
      for rkey, rvalue in pairs(inmap) do
         local idxdb = idxdbs[rkey]
         if idxdb then
            local idxkey = rvalue .. " " .. id
            if not idxdb:set(idxkey, "") then
               kt.log("error", "setting an index entry failed")
               err = true
            end
         end
      end
      -- insert the serialized data into the main database
      return serial
   end
   -- perform the visitor atomically
   if not db:accept(id, visit) then
      kt.log("error", "inserting a record failed")
      err = true
   end
   if err then
      return kt.EVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- get a record
function get(inmap, outmap)
   local id = tostring(inmap.id)
   if not id then
      return kt.RVEINVALID
   end
   local serial = db:get(id)
   if not serial then
      return kt.RVELOGIC
   end
   local rec = kt.mapload(serial)
   for rkey, rvalue in pairs(rec) do
      outmap[rkey] = rvalue
   end
   return kt.RVSUCCESS
end

-- get heading records
function head(inmap, outmap)
   local name = tostring(inmap.name)
   if not name then
      return kt.RVEINVALID
   end
   local max = tonumber(inmap.max)
   if not max then
      max = 10
   end
   local idxdb = idxdbs[name]
   if not idxdb then
      return kt.RVELOGIC
   end
   local cur = idxdb:cursor()
   cur:jump()
   local rec
   while max > 0 do
      local key = cur:get_key(true)
      if not key then
         break
      end
      local rkey = kt.regex(key, "[^ ]+ ", "")
      local rvalue = kt.regex(key, " .*", "")
      outmap[rkey] = rvalue
      max = max - 1
   end
   cur:disable()
   return kt.RVSUCCESS
end

-- get tailing records
function tail(inmap, outmap)
   local name = tostring(inmap.name)
   if not name then
      return kt.RVEINVALID
   end
   local max = tonumber(inmap.max)
   if not max then
      max = 10
   end
   local idxdb = idxdbs[name]
   if not idxdb then
      return kt.RVELOGIC
   end
   local cur = idxdb:cursor()
   cur:jump_back()
   local rec
   while max > 0 do
      local key = cur:get_key()
      if not key then
         break
      end
      local rkey = kt.regex(key, "[^ ]+ ", "")
      local rvalue = kt.regex(key, " .*", "")
      outmap[rkey] = rvalue
      cur:step_back()
      max = max - 1
   end
   cur:disable()
   return kt.RVSUCCESS
end

-- reindex an index
function reindex(inmap, outmap)
   local name = tostring(inmap.name)
   if not name then
      return kt.RVEINVALID
   end
   local idxdb = idxdbs[name]
   if not idxdb then
      return kt.RVELOGIC
   end
   local err = false
   -- map function: invert the record data
   local function map(key, value, emit)
      local obj = kt.mapload(value)
      for rkey, rvalue in pairs(obj) do
         local idxdb = idxdbs[rkey]
         if idxdb then
            emit(rvalue, key)
         end
      end
      return true
   end
   -- reduce function: insert into the index
   local function reduce(key, iter)
      local value
      while true do
         value = iter()
         if not value then
            break
         end
         local idxkey = key .. " " .. value
         if not idxdb:set(idxkey, "") then
            kt.log("error", "setting an index entry failed")
            err = true
         end
      end
      if err then
         return false
      end
      return true
   end
   -- clear the index
   if not idxdb:clear() then
      kt.log("error", "clearing an index failed")
      err = true
   end
   -- update the index
   if not db:mapreduce(map, reduce) then
      kt.log("error", "mapreduce failed")
      err = true
   end
   if err then
      return kt.EVEINTERNAL
   end
   return kt.RVSUCCESS
end

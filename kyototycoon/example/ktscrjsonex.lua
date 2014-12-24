kt = __kyototycoon__
db = kt.db
json = require("json")

-- post a new document
function writedoc(inmap, outmap)
   local doc = inmap.doc
   if not doc then
      return kt.RVEINVALID
   end
   local doc = json.decode(doc)
   local owner = tonumber(doc.owner)
   local date = tonumber(doc.date)
   local title = tostring(doc.title)
   local text = tostring(doc.text)
   local key = string.format("%010d:%010d", owner, date)
   local doc = {}
   doc.title = title
   doc.text = text
   local value = json.encode(doc)
   if not db:add(key, value) then
      local err = db:error()
      if err:code() == kt.Error.DUPREC then
         return kt.RVELOGIC
      end
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- post a new comment
function addcomment(inmap, outmap)
   local owner = tonumber(inmap.owner)
   local date = tonumber(inmap.date)
   local comowner = tonumber(inmap.comowner)
   local comdate = tostring(inmap.comdate)
   local comtext = tostring(inmap.comtext)
   if not owner or not date or not comowner or not comdate or not comtext then
      return kt.RVEINVALID
   end
   local key = string.format("%010d:%010d", owner, date)
   local hit = false
   local function visit(rkey, rvalue)
      local doc = json.decode(rvalue)
      if not doc.comments then
         doc.comments = {}
      end
      local newcom = {}
      newcom.owner = comowner
      newcom.date = comdate
      newcom.text = comtext
      table.insert(doc.comments, newcom)
      hit = true
      return json.encode(doc)
   end
   if not db:accept(key, visit) then
      local err = db:error()
      if err:code() == kt.Error.DUPREC then
         return kt.RVELOGIC
      end
      return kt.RVEINTERNAL
   end
   if not hit then
      return kt.RVELOGIC
   end
   return kt.RVSUCCESS
end

-- get a document
function getdoc(inmap, outmap)
   local owner = tonumber(inmap.owner)
   local date = tonumber(inmap.date)
   if not owner or not date then
      return kt.RVEINVALID
   end
   local key = string.format("%010d:%010d", owner, date)
   local value = db:get(key)
   if not value then
      return kt.RVELOGIC
   end
   local doc = _composedoc(key, value)
   outmap.doc = json.encode(doc)
   return kt.RVSUCCESS
end

-- list documents of a user
function listnewdocs(inmap, outmap)
   local owner = tonumber(inmap.owner)
   local max = tonumber(inmap.max)
   if not owner then
      return kt.RVEINVALID
   end
   if not max then
      max = 10
   end
   local key = string.format("%010d:", owner)
   local cur = db:cursor()
   cur:jump_back(key .. "~")
   local num = 0
   while num < max do
      local rkey, rvalue = cur:get()
      if not rkey or not kt.strfwm(rkey, key) then
         break
      end
      cur:step_back()
      local doc = _composedoc(rkey, rvalue)
      num = num + 1
      outmap[num] = json.encode(doc)
   end
   return kt.RVSUCCESS
end

-- list documents of a user without the text and the comments
function listnewdocslight(inmap, outmap)
   local owner = tonumber(inmap.owner)
   local max = tonumber(inmap.max)
   if not owner then
      return kt.RVEINVALID
   end
   if not max then
      max = 10
   end
   local key = string.format("%010d:", owner)
   local cur = db:cursor()
   cur:jump_back(key .. "~")
   local num = 0
   while num < max do
      local rkey, rvalue = cur:get()
      if not rkey or not kt.strfwm(rkey, key) then
         break
      end
      cur:step_back()
      local doc = _composedoc(rkey, rvalue)
      doc.text = nil
      doc.comnum = #doc.comments
      doc.comments = nil
      num = num + 1
      outmap[num] = json.encode(doc)
   end
   return kt.RVSUCCESS
end

-- helper to compose a document object
function _composedoc(key, value)
   local doc = json.decode(value)
   doc.owner = string.gsub(key, ":%d*", "")
   doc.date = string.gsub(key, "%d*:", "")
   if not doc.comments then
      doc.comments = {}
   end
   return doc
end

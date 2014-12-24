--
-- refine the documents
--

INDEXFILE = "doc/luadoc/index.html"
MODULEFILE = "doc/luadoc/modules/kyototycoon.html"
CSSFILE = "doc/luadoc/luadoc.css"
TMPFILE = INDEXFILE .. "~"
OVERVIEWFILE = "luaoverview.html"

ofd = io.open(TMPFILE, "wb")
first = true
for line in io.lines(INDEXFILE) do
   if first and string.match(line, "<h2> *Modules *</h2>") then
      for tline in io.lines(OVERVIEWFILE) do
         ofd:write(tline .. "\n")
      end
      first = false
   end
   ofd:write(line .. "\n")
end
ofd:close()
os.remove(INDEXFILE)
os.rename(TMPFILE, INDEXFILE)

ofd = io.open(TMPFILE, "wb")
first = true
for line in io.lines(MODULEFILE) do
   if string.match(line, "<em>Fields</em>") then
      ofd:write("<br/>\n")
   end
   ofd:write(line .. "\n")
end
ofd:close()
os.remove(MODULEFILE)
os.rename(TMPFILE, MODULEFILE)

ofd = io.open(TMPFILE, "wb")
for line in io.lines(CSSFILE) do
   if not string.match(line, "#TODO:") then
      ofd:write(line .. "\n")
   end
end
ofd:write("table.function_list td.name { width: 20em; }\n")
ofd:close()
os.remove(CSSFILE)
os.rename(TMPFILE, CSSFILE)

require 'base64'

rtspath = "ktreplprint.rts"

mode = File::Constants::RDWR | File::Constants::CREAT
File::open(rtspath, mode) do |rtsfile|
  while true
    begin
      line = $stdin.readline
    rescue
      break
    end
    line = line.strip
    fields = line.split("\t")
    next if fields.length < 4
    rts = fields[0]
    rsid = fields[1]
    rdbid = fields[2]
    rcmd = fields[3]
    args = []
    i = 4
    while i < fields.length
      args.push(fields[i].unpack("m")[0])
      i += 1
    end
    printf("ts=%d sid=%d dbid=%d: ", rts, rsid, rdbid)
    case rcmd
    when "set"
      if args.length >= 2
        key = args[0]
        value = args[1][5,args[1].length]
        nums = args[1].unpack("C5")
        xt = 0
        nums.each do |num|
          xt = (xt << 8) + num
        end
        printf("set: key=%s value=%s xt=%d", key, value, xt)
      end
    when "remove"
      if args.length >= 1
        key = args[0]
        printf("remove: key=%s", key)
      end
    when "clear"
      printf("clear")
    end
    printf("\n")
    rtsfile.pos = 0
    rtsfile.printf("%020d\n", rts)
  end
end

exit 0

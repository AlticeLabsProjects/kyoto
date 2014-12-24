require 'dbi'
require 'base64'

hostname = "localhost"
dbname = "kttest"
username = "root"
password = ""
rtspath = "ktreplmysql.rts"

# $ mysql --user=root
# > create database kttest;
# > create table kttest (
#   k varchar(256) primary key,
#   v varchar(256) not null,
#   xt datetime not null
# ) TYPE=InnoDB;

begin
  dbh = DBI::connect("dbi:Mysql:#{dbname}:#{hostname}", username, password)
  sthins = dbh.prepare("INSERT INTO kttest ( k, v, xt ) VALUES ( ?, ?, ? )" +
                       " ON DUPLICATE KEY UPDATE v = ?, xt = ?;")
  sthrem = dbh.prepare("DELETE FROM kttest WHERE k = ?;")
  sthclr = dbh.prepare("DELETE FROM kttest;")
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
          xt = 1 << 32 if xt > (1 << 32)
          xt = Time::at(xt).strftime("%Y-%m-%d %H:%M:%S")
          sthins.execute(key, value, xt, value, xt)
        end
      when "remove"
        if args.length >= 1
          key = args[0]
          sthrem.execute(key)
        end
      when "clear"
        sthclr.execute()
      end
      rtsfile.pos = 0
      rtsfile.printf("%020d\n", rts)
    end
  end
rescue Exception => e
  printf("Error: %s\n", e)
ensure
  dbh.disconnect if dbh
end

exit 0

require 'uri'
require 'net/http'

# RESTful interface of Kyoto Tycoon
class KyotoTycoon
  # connect to the server
  def open(host = "127.0.0.1", port = 1978, timeout = 30)
    @ua = Net::HTTP::new(host, port)
    @ua.read_timeout = timeout
    @ua.start
  end
  # close the connection
  def close
    @ua.finish
  end
  # store a record
  def set(key, value, xt = nil)
    key = "/" + URI::encode(key)
    req = Net::HTTP::Put::new(key)
    if xt
      xt = Time::now.to_i + xt
      req.add_field("X-Kt-Xt", xt)
    end
    res = @ua.request(req, value)
    res.code.to_i == 201
  end
  # remove a record
  def remove(key)
    key = "/" + URI::encode(key)
    req = Net::HTTP::Delete::new(key)
    res = @ua.request(req)
    res.code.to_i == 204
  end
  # retrieve the value of a record
  def get(key)
    key = "/" + URI::encode(key)
    req = Net::HTTP::Get::new(key)
    res = @ua.request(req)
    return nil if res.code.to_i != 200
    res.body
  end
end

# sample usage
kt = KyotoTycoon::new
kt.open("localhost", 1978)
kt.set("japan", "tokyo", 60)
printf("%s\n", kt.get("japan"))
kt.remove("japan")
kt.close

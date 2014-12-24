import time
import urllib
import http.client

# RESTful interface of Kyoto Tycoon
class KyotoTycoon:
    # connect to the server
    def open(self, host = "127.0.0.1", port = 1978, timeout = 30):
        self.ua = http.client.HTTPConnection(host, port, False, timeout)
    # close the connection
    def close(self):
        self.ua.close()
    # store a record
    def set(self, key, value, xt = None):
        if isinstance(key, str): key = key.encode("UTF-8")
        if isinstance(value, str): value = value.encode("UTF-8")
        key = "/" + urllib.parse.quote(key)
        headers = {}
        if xt != None:
            xt = int(time.time()) + xt
            headers["X-Kt-Xt"] = str(xt)
        self.ua.request("PUT", key, value, headers)
        res = self.ua.getresponse()
        body = res.read()
        return res.status == 201
    # remove a record
    def remove(self, key):
        if isinstance(key, str): key = key.encode("UTF-8")
        key = "/" + urllib.parse.quote(key)
        self.ua.request("DELETE", key)
        res = self.ua.getresponse()
        body = res.read()
        return res.status == 204
    # retrieve the value of a record
    def get(self, key):
        if isinstance(key, str): key = key.encode("UTF-8")
        key = "/" + urllib.parse.quote(key)
        self.ua.request("GET", key)
        res = self.ua.getresponse()
        body = res.read()
        if res.status != 200: return None
        return body

# sample usage
kt = KyotoTycoon()
kt.open("localhost", 1978)
kt.set("japan", "tokyo", 60)
print(kt.get("japan"))
kt.remove("japan")
kt.close()

require 'memcache'

host = "127.0.0.1:11211"
options = {
  :timeout => 3600,
}

printf("connecting...\n")
cache = MemCache.new([host], options)
printf("count: %s\n", cache.stats[host]["curr_items"])

printf("flusing...\n")
cache.flush_all
printf("count: %s\n", cache.stats[host]["curr_items"])

printf("setting...\n")
(1..10).each do |i|
  str = i.to_s
  cache.set(str, str, 180, { :raw => true })
  cache.add(str, str, 180, { :raw => true })
  cache.replace(str, str, 180, { :raw => true })
end
printf("count: %s\n", cache.stats[host]["curr_items"])

printf("incrementing...\n")
(1..10).each do |i|
  str = i.to_s
  cache.incr(str, 10000)
  cache.decr(str, 1000)
end
printf("count: %s\n", cache.stats[host]["curr_items"])

printf("getting...\n")
(1..10).each do |i|
  str = i.to_s
  value = cache.get(str, { :raw => true })
  printf("get: %s: %s\n", str, value)
end
printf("count: %s\n", cache.stats[host]["curr_items"])

printf("removing...\n")
(1..10).each do |i|
  str = i.to_s
  cache.delete(str)
end
printf("count: %s\n", cache.stats[host]["curr_items"])

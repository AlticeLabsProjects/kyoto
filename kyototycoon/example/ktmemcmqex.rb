require 'memcache'

host = "127.0.0.1:11211"
options = {
  :timeout => 3600,
}
thnum = 4
rnum = 100

gcache = MemCache.new([host], options)
gcache.flush_all

producers = Array::new
for thid in 0...thnum
  th = Thread::new(thid) { |id|
    cache = MemCache.new([host], options)
    mid = rnum / 4
    for i in 0...rnum
      name = (i % 10).to_s
      value = i.to_s
      printf("set: %s: %s\n", name, value)
      cache.set(name, value, 0, { :raw => true })
      wt = rand() * 0.1
      sleep(wt) if i >= mid && wt >= 0.01
    end
  }
  producers.push(th)
end

workers = Array::new
for thid in 0...thnum
  th = Thread::new(thid) { |id|
    cache = MemCache.new([host], options)
    for i in 0...rnum
      name = (i % 10).to_s
      value = cache.get(name, { :raw => true })
      printf("get: %s: %s\n", name, value ? value : "(miss)")
      cache.delete(name)
    end
  }
  workers.push(th)
end

workers.each { |th|
  th.join
}
producers.each { |th|
  th.join
}
GC.start

printf("count: %s\n", gcache.stats[host]["curr_items"])

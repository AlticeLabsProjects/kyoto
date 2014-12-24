use strict;
use warnings;
use Cache::Memcached::Fast;

my $servers = [{
    address => "127.0.0.1:11211",
    noreply => 1,
}];

printf("connecting...\n");
my $cache = Cache::Memcached::Fast->new({ servers => $servers });
if (!defined($cache)) {
    printf("new failed\n");
}

printf("checking the version...\n");
my $versions = $cache->server_versions();
while (my ($server, $version) = each(%$versions)) {
    printf("versions: %s: %s\n", $server, $version);
}

printf("flusing...\n");
if (!$cache->flush_all()) {
    printf("flush_all failed\n");
}

printf("flusing without reply...\n");
$cache->flush_all();

printf("setting...\n");
for (my $i = 1; $i <= 10; $i++) {
    if (!$cache->set($i, $i)) {
        printf("set failed\n");
    }
}

printf("incrementing...\n");
for (my $i = 1; $i <= 10; $i++) {
    if (!defined($cache->incr($i, 10000))) {
        printf("incr failed\n");
    }
    if (!defined($cache->decr($i, 1000))) {
        printf("decr failed\n");
    }
}

printf("retrieving...\n");
for (my $i = 1; $i <= 10; $i++) {
    my $value = $cache->get($i);
    printf("get: %s: %s\n", $i, $value);
}

printf("removing...\n");
for (my $i = 1; $i <= 10; $i++) {
    if (!$cache->remove($i)) {
        printf("remove failed\n");
    }
}

printf("setting without reply...\n");
for (my $i = 1; $i <= 10; $i++) {
    $cache->set($i, $i);
}

printf("incrementing without reply...\n");
for (my $i = 1; $i <= 10; $i++) {
    $cache->incr($i, 1000);
}

printf("retrieving in bulk...\n");
my $values = $cache->get_multi(1..10);
while (my ($key, $value) = each(%$values)) {
    printf("get_multi: %s: %s\n", $key, $value);
}

printf("removing without reply...\n");
for (my $i = 1; $i <= 10; $i++) {
    $cache->delete($i);
}

printf("disconnecting...\n");
$cache->disconnect_all();

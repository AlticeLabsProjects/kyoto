use strict;
use warnings;
use LWP::UserAgent;
use URI::Escape;

{
    # RESTful interface of Kyoto Tycoon
    package KyotoTycoon;

    # constructor
    sub new {
        my $self = {};
        bless $self;
        return $self;
    }

    # connect to the server
    sub open {
        my ($self, $host, $port, $timeout) = @_;
        $host = "127.0.0.1" if (!defined($host));
        $port = 1978 if (!defined($port));
        $timeout = 30 if (!defined($timeout));
        $self->{base} = "http://$host:$port/";
        $self->{ua} = LWP::UserAgent->new(keep_alive => 1);
        $self->{ua}->timeout($timeout);
        return undef;
    }

    # close the connection
    sub close {
        my ($self) = @_;
        $self->{ua} = undef;
        return undef;
    }

    # store a record
    sub set {
        my ($self, $key, $value, $xt) = @_;
        my $url = $self->{base} . URI::Escape::uri_escape($key);
        my @headers;
        if (defined($xt)) {
            $xt = time() + $xt;
            push(@headers, "X-Kt-Xt");
            push(@headers, $xt);
        }
        my $req = HTTP::Request->new(PUT => $url, \@headers, $value);
        my $res = $self->{ua}->request($req);
        my $code = $res->code();
        return $code == 201;
    }

    # remove a record
    sub remove {
        my ($self, $key) = @_;
        my $url = $self->{base} . URI::Escape::uri_escape($key);
        my $req = HTTP::Request->new(DELETE => $url);
        my $res = $self->{ua}->request($req);
        my $code = $res->code();
        return $code == 204;
    }

    # retrieve the value of a record
    sub get {
        my ($self, $key) = @_;
        my $url = $self->{base} . URI::Escape::uri_escape($key);
        my $req = HTTP::Request->new(GET => $url);
        my $res = $self->{ua}->request($req);
        my $code = $res->code();
        return undef if ($code != 200);
        return $res->content();
    }

}

# sample usage
my $kt = new KyotoTycoon;
$kt->open("localhost", 1978);
$kt->set("japan", "tokyo", 30);
printf("%s\n", $kt->get("japan"));
$kt->remove("japan");
$kt->close();

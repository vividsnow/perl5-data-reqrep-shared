use strict;
use warnings;
use Test::More;
use POSIX ();
use File::Temp qw(tempdir);
use Time::HiRes qw(time sleep);
use Data::ReqRep::Shared;
use Data::ReqRep::Shared::Client;

# Small requests arriving back to back must not keep a request that needs most
# of the arena out for good.

my $p = tempdir(CLEANUP => 1) . '/fair.shm';
my @geom = ($p, 1024, 1024, 16, 256);
my $srv = Data::ReqRep::Shared->new(@geom);
my $stop = time + 20;
my @kids;
for (1 .. 4) {
    my $pid = fork // die $!;
    if (!$pid) {
        my $c = Data::ReqRep::Shared::Client->new($p);
        while (time < $stop) { my $id = $c->send_wait('s' x 10, 1); $c->cancel($id) if defined $id }
        POSIX::_exit(0);
    }
    push @kids, $pid;
}
my $drain = fork // die $!;
if (!$drain) {
    my $s = Data::ReqRep::Shared->new(@geom);
    while (time < $stop) { $s->recv_wait(0.1); sleep 0.002 }
    POSIX::_exit(0);
}
push @kids, $drain;
sleep 0.3;

my $c = Data::ReqRep::Shared::Client->new($p);
my $in = 0;
for (1 .. 5) {
    my $id = $c->send_wait('B' x 240, 3);
    next unless defined $id;
    $in++;
    $c->cancel($id);
}
is $in, 5, 'a request needing most of the arena gets in, every time, while small ones keep coming';

kill KILL => @kids;
waitpid $_, 0 for @kids;
for my $how (qw(killed timed_out)) {
    my $q = tempdir(CLEANUP => 1) . "/reserve_$how.shm";
    my $s = Data::ReqRep::Shared->new($q, 1024, 1024, 16, 256);
    my $c = Data::ReqRep::Shared::Client->new($q);
    my $queued = 0;
    $queued++ while defined $c->send('s' x 10);
    my $big = fork // die $!;
    if (!$big) {
        my $bc = Data::ReqRep::Shared::Client->new($q);
        $bc->send_wait('B' x 4090, $how eq 'killed' ? 30 : 0.1);
        sleep 30;
        POSIX::_exit(0);
    }
    sleep 0.3;
    if ($how eq 'killed') { kill KILL => $big; waitpid $big, 0 }
    $s->recv for 1 .. $queued;
    my ($sent, $t0) = (0, time);
    until ($sent or $how ne 'killed' or time - $t0 > 2) { $sent = defined $c->send('s' x 10); sleep 0.005 unless $sent }
    $sent ||= defined $c->send('s' x 10);
    ok $sent, $how eq 'killed' ? 'a sender killed while holding arena room for its request does not hold it for good'
                   : 'a sender whose wait for arena room timed out gives the room back at once';
    if ($how ne 'killed') { kill KILL => $big; waitpid $big, 0 }
}

done_testing;

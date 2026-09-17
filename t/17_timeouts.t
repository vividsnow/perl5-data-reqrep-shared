use strict;
use warnings;
use Test::More;
use Time::HiRes qw(time);
use File::Temp ();
use Data::ReqRep::Shared;
use Data::ReqRep::Shared::Client;
use Data::ReqRep::Shared::Int;
use Data::ReqRep::Shared::Int::Client;

sub sleeps {
    open my $fh, '<', '/proc/self/status' or die $!;
    while (<$fh>) { return $1 if /^voluntary_ctxt_switches:\s+(\d+)/ }
    die "no voluntary_ctxt_switches in /proc/self/status";
}

# A wait that re-arms a zero timeout costs little CPU but sleeps thousands of times.
sub waits_for {
    my ($secs, $name, $code) = @_;
    my $n0 = sleeps();
    my $t0 = time;
    $code->();
    my $wall = time - $t0;
    my $n = sleeps() - $n0;
    cmp_ok $wall, '>=', $secs * 0.9, "$name waits out its timeout";
    cmp_ok $n, '<', 50, "  in one sleep, not a loop of them";
}

for my $name (qw(Str Int)) {
    my $int = $name eq 'Int';
    my $srv = $int ? Data::ReqRep::Shared::Int->new_memfd('t', 2, 1) : Data::ReqRep::Shared->new_memfd('t', 2, 1, 64);
    my $cli = ($int ? 'Data::ReqRep::Shared::Int::Client' : 'Data::ReqRep::Shared::Client')->new_from_fd($srv->memfd);
    my $msg = $int ? 7 : 'x';

    waits_for(0.4, "$name recv_wait", sub { $srv->recv_wait(0.4) });
    waits_for(0.4, "$name req_wait", sub { $cli->req_wait($msg, 0.4) });
    my $id = $cli->send($msg);
    waits_for(0.4, "$name get_wait", sub { $cli->get_wait($id, 0.4) });
    waits_for(0.4, "$name send_wait", sub { $cli->send_wait($msg, 0.4) });
}

# A slot naming no process must not make a send wait for it.
for my $case ([ACQUIRED => 1, 0], [ABANDONED => 5, 0]) {
    my ($label, $state, $pid) = @$case;
    my $p = File::Temp::tempdir(CLEANUP => 1) . '/stuck.shm';
    my $srv = Data::ReqRep::Shared->new($p, 16, 1, 64);
    open my $fh, '+<', $p or die $!;
    binmode $fh;
    seek $fh, 44, 0; read $fh, my $off, 4; $off = unpack 'L', $off;
    seek $fh, $off, 0; print {$fh} pack 'Q', $pid << 8 | $state;
    close $fh;
    my $cli = Data::ReqRep::Shared::Client->new($p);
    my $t0 = time;
    my $id = $cli->send_wait('x', 0);
    cmp_ok time - $t0, '<', 0.5, "send_wait with timeout 0 returns at once beside an $label slot naming no process";
}

done_testing;

use strict;
use warnings;
use Test::More;
use POSIX ();
use Time::HiRes qw(time sleep);
use Data::ReqRep::Shared;
use Data::ReqRep::Shared::Client;

plan skip_all => 'Linux only' unless $^O eq 'linux';

sub failed_send_cost {
    my ($slots, $n) = @_;
    my $s = Data::ReqRep::Shared->new_memfd('scan', 2 * $slots, $slots, 64);
    pipe my $r, my $w or die $!;
    my $holder = fork // die $!;
    if (!$holder) {
        my $c = Data::ReqRep::Shared::Client->new_from_fd($s->memfd);
        $c->send('x') for 1 .. $slots;
        syswrite $w, 'x';
        sleep 60;
        POSIX::_exit(0);
    }
    close $w;
    sysread $r, my $go, 1;
    my $c = Data::ReqRep::Shared::Client->new_from_fd($s->memfd);
    my $t0 = time;
    for (1 .. $n) { defined $c->send('y') and die "a send succeeded with every slot held" }
    my $each = (time - $t0) / $n;
    kill KILL => $holder;
    waitpid $holder, 0;
    return $each;
}

my $few  = failed_send_cost(4, 20000);
my $many = failed_send_cost(64, 20000);
diag sprintf 'failed send: %.1f us with 4 slots held, %.1f us with 64', $few * 1e6, $many * 1e6;
cmp_ok $many * 1e6, '<', 20, 'a failed send with 64 held slots stays cheap';

# A scan of this many live holders takes longer than the throttle interval itself.
my $lots = failed_send_cost(8192, 400);
diag sprintf 'failed send: %.1f us with 8192 slots held', $lots * 1e6;
cmp_ok $lots * 1e6, '<', 1000, 'the throttle still holds when one scan outlasts its interval';

# Every slot of a killed client is recovered in about one scan, not one scan per slot.
{
    my $n = 2048;
    my $s = Data::ReqRep::Shared->new_memfd('dead', 2 * $n, $n, 64);
    my $holder = fork // die $!;
    if (!$holder) {
        my $c = Data::ReqRep::Shared::Client->new_from_fd($s->memfd);
        $c->send('x') for 1 .. $n;
        POSIX::_exit(0);
    }
    waitpid $holder, 0;
    my $c = Data::ReqRep::Shared::Client->new_from_fd($s->memfd);
    my $t0 = time;
    my $sent = grep { defined $c->send('y') } 1 .. $n;
    my $took = time - $t0;
    is $sent, $n, "all $n slots of a killed client are recovered";
    cmp_ok $took, '<', 1, sprintf '  in well under a second (took %.2f s)', $took;
}

done_testing;

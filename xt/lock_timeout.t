use strict;
use warnings;
use Test::More;
use Fcntl qw(:flock);
use File::Temp qw(tempdir);
use POSIX ();
use Time::HiRes qw(time);
use Data::ReqRep::Shared;
use Data::ReqRep::Shared::Client;

my $path = tempdir(CLEANUP => 1) . '/held.shm';
Data::ReqRep::Shared->new($path, 4, 2, 64);

pipe my $r, my $w or die $!;
my $holder = fork // die $!;
if (!$holder) {
    open my $fh, '<', $path or die $!;
    flock $fh, LOCK_EX or die $!;
    syswrite $w, 'x';
    sleep 60;
    POSIX::_exit(0);
}
close $w;
sysread $r, my $go, 1;

for my $case (['new', sub { Data::ReqRep::Shared->new($path, 4, 2, 64) }],
              ['a client', sub { Data::ReqRep::Shared::Client->new($path) }]) {
    my $t0 = time;
    ok !eval { $case->[1]->(); 1 }, "$case->[0] gives up on a lock another process holds";
    my $took = time - $t0;
    cmp_ok $took, '<', 15, sprintf '  after the documented 10 s (took %.1f s)', $took;
    cmp_ok $took, '>', 8, '  not before it';
}

kill KILL => $holder;
waitpid $holder, 0;
done_testing;

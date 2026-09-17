use strict;
use warnings;
use Test::More;
use File::Temp qw(tempdir);
use POSIX ();
use Time::HiRes qw(time sleep);
use Data::ReqRep::Shared;
use Data::ReqRep::Shared::Client;
use Data::ReqRep::Shared::Int;
use Data::ReqRep::Shared::Int::Client;

# Parked callers run in children, so a regression fails instead of hanging.

my $dir = tempdir(CLEANUP => 1);
my %kind = (
    Str => { server => sub { Data::ReqRep::Shared->new($_[0], 16, $_[1], 64) },
             client => sub { Data::ReqRep::Shared::Client->new($_[0]) }, msg => 'x' },
    Int => { server => sub { Data::ReqRep::Shared::Int->new($_[0], 16, $_[1]) },
             client => sub { Data::ReqRep::Shared::Int::Client->new($_[0]) }, msg => 7 },
);

sub within {
    my ($secs, $cond) = @_;
    my $end = time + $secs;
    my $v;
    sleep 0.02 until ($v = $cond->()) || time > $end;
    return $v;
}

sub parked_sender {
    my ($k, $p) = @_;
    my $pid = fork // die $!;
    if (!$pid) { POSIX::_exit(defined $k->{client}->($p)->send_wait($k->{msg}) ? 0 : 1) }
    return $pid;
}

# How many of @pids exit successfully within $secs; the rest are killed.
sub succeed_within {
    my ($secs, @pids) = @_;
    my %left = map { $_ => 1 } @pids;
    my $ok = 0;
    within($secs, sub {
        for my $pid (keys %left) {
            next unless waitpid($pid, POSIX::WNOHANG()) > 0;
            delete $left{$pid};
            $ok++ if $? == 0;
        }
        !%left;
    });
    for (keys %left) { kill KILL => $_; waitpid $_, 0 }
    return $ok;
}

for my $name (qw(Str Int)) {
    my $k = $kind{$name};

    subtest "$name: a sender parked for a slot wakes when the holders die" => sub {
        my $p = "$dir/dead_$name.shm";
        my $srv = $k->{server}->($p, 2);
        my @holders = map {
            my $pid = fork // die $!;
            if (!$pid) { my $c = $k->{client}->($p); $c->send($k->{msg}); sleep 60; POSIX::_exit(0) }
            $pid;
        } 1 .. 2;
        within(5, sub { $srv->size == 2 }) or diag 'holders never sent';
        my $w = parked_sender($k, $p);
        within(5, sub { $srv->stats->{slot_waiters} }) or diag 'sender never parked';
        kill KILL => @holders;
        waitpid $_, 0 for @holders;
        is succeed_within(6, $w), 1, 'it gets a slot without waiting for a release';
    };

    subtest "$name: back-to-back sends recover every slot the dead left" => sub {
        my $p = "$dir/burst_$name.shm";
        my $srv = $k->{server}->($p, 4);
        for (1 .. 4) {
            my $pid = fork // die $!;
            if (!$pid) { my $c = $k->{client}->($p); $c->send($k->{msg}); POSIX::_exit(0) }
            waitpid $pid, 0;
        }
        my $c = $k->{client}->($p);
        is scalar(grep { defined $c->send($k->{msg}) } 1 .. 4), 4, 'four sends in a row get four slots';
    };

    subtest "$name: a slot held by a child forked after the parent used the channel is recovered" => sub {
        my $p = "$dir/fork_$name.shm";
        my $srv = $k->{server}->($p, 1);
        my $c = $k->{client}->($p);
        my $id = $c->send($k->{msg});
        my ($m, $rid) = $srv->recv;
        $srv->reply($rid, $m);
        $c->get($id);
        my $pid = fork // die $!;
        if (!$pid) { $c->send($k->{msg}); POSIX::_exit(0) }
        waitpid $pid, 0;
        ok defined $c->send($k->{msg}), 'the parent gets the slot the dead child held';
    };

    subtest "$name: clear wakes every parked sender" => sub {
        my $p = "$dir/clear_$name.shm";
        my $srv = $k->{server}->($p, 4);
        my $holder = fork // die $!;
        if (!$holder) { my $c = $k->{client}->($p); $c->send($k->{msg}) for 1 .. 4; sleep 60; POSIX::_exit(0) }
        within(5, sub { $srv->size == 4 }) or diag 'holder never sent';
        my @w = map { parked_sender($k, $p) } 1 .. 4;
        within(5, sub { $srv->stats->{slot_waiters} == 4 }) or diag 'senders never parked';
        $srv->clear;
        is succeed_within(1, @w), 4, 'all of them get a slot at once';
        kill KILL => $holder;
        waitpid $holder, 0;
    };
}

# A receive that frees room for several blocked senders releases all of them.
for my $mode (qw(drain recv_multi recv_wait_multi arena)) {
    subtest "Str: $mode releases every sender it made room for" => sub {
        my $arena = $mode eq 'arena';
        my $srv = $arena ? Data::ReqRep::Shared->new_memfd('bw', 64, 64, 16, 4096)
                         : Data::ReqRep::Shared->new_memfd('bw', 4, 64, 16);
        my $fill = Data::ReqRep::Shared::Client->new_from_fd($srv->memfd);
        if ($arena) { $fill->send('F' x 4088) } else { $fill->send("fill$_") for 1 .. 4 }
        my $m = $arena ? 'x' x 64 : 'x';
        ok !defined $fill->send($m), 'the queue is full';
        my @kids;
        for (1 .. 6) {
            pipe my $r, my $w or die $!;
            my $pid = fork // die $!;
            if (!$pid) {
                close $r;
                my $c = Data::ReqRep::Shared::Client->new_from_fd($srv->memfd);
                my $id = $c->send_wait($m, 2);
                syswrite $w, sprintf "%d %.3f\n", defined $id ? 1 : 0, time;
                POSIX::_exit(0);
            }
            close $w;
            push @kids, [$pid, $r];
        }
        within(3, sub { $srv->stats->{send_waiters} == 6 }) or diag 'senders did not all park';
        my $t_free = time;
        if    ($mode eq 'drain')           { my @x = $srv->drain }
        elsif ($mode eq 'recv_multi')      { my @x = $srv->recv_multi(4) }
        elsif ($mode eq 'recv_wait_multi') { my @x = $srv->recv_wait_multi(4, 1) }
        else                               { my @x = $srv->recv }
        my $quick = 0;
        for my $k (@kids) {
            waitpid $k->[0], 0;
            my ($ok, $t) = split ' ', readline $k->[1];
            $quick++ if $ok && $t - $t_free < 0.8;
        }
        is $quick, $arena ? 6 : 4, $arena ? 'all six fit and all six go at once' : 'four fit and four go at once';
    };
}

for my $name (qw(Str Int)) {
    my $k = $kind{$name};
    subtest "$name: a waiter gives up when the process that received its request dies" => sub {
        my $p = "$dir/dead_receiver_$name.shm";
        my $srv = $k->{server}->($p, 1);
        my $waiter = fork // die $!;
        if (!$waiter) {
            my $r = $k->{client}->($p)->req($k->{msg});
            POSIX::_exit(defined $r ? 1 : 0);
        }
        my $worker = fork // die $!;
        if (!$worker) { my @m = $k->{server}->($p, 1)->recv_wait(5); POSIX::_exit(@m ? 0 : 3) }
        waitpid $worker, 0;
        is $? >> 8, 0, 'the worker took the request and exited without replying';
        is succeed_within(8, $waiter), 1, 'req() with no timeout returns undef instead of waiting for ever';
        ok defined $k->{client}->($p)->send($k->{msg}), '  and the slot is free again';
    };

    subtest "$name: only the process that received a request may reply to it" => sub {
        my $p = "$dir/same_process_$name.shm";
        my $srv = $k->{server}->($p, 1);
        my $cli = $k->{client}->($p);
        my $id = $cli->send($k->{msg});
        my (undef, $rid) = $srv->recv;
        my $other = fork // die $!;
        if (!$other) { POSIX::_exit($k->{server}->($p, 1)->reply($rid, $k->{msg}) ? 1 : 0) }
        waitpid $other, 0;
        is $? >> 8, 0, 'a reply from another process is refused';
        ok $srv->reply($rid, $k->{msg}), 'the receiving process replies';
        is $cli->get_wait($id, 2), $k->{msg}, '  and the client gets that reply';
    };
}

for my $name (qw(Str Int)) {
    my $int = $name eq 'Int';
    my $msg = $int ? 7 : 'x';
    subtest "$name: a waiter killed while parked stops being counted once wakes find nobody" => sub {
        my $p = "$dir/stale_waiter_$name.shm";
        my $srv = $int ? Data::ReqRep::Shared::Int->new($p, 2, 3) : Data::ReqRep::Shared->new($p, 2, 3, 64);
        my $cc = $int ? 'Data::ReqRep::Shared::Int::Client' : 'Data::ReqRep::Shared::Client';
        my $round_trips = sub {
            my $cli = $cc->new($p);
            for (1 .. 32) {
                my $id = $cli->send($msg) // return 0;
                my (undef, $rid) = $srv->recv or return 0;
                $srv->reply($rid, $msg);
                defined $cli->get($id) or return 0;
            }
            1;
        };
        my $killed_while = sub {
            my ($counter, $park) = @_;
            my $pid = fork // die $!;
            if (!$pid) { $park->(); POSIX::_exit(0) }
            within(5, sub { $srv->stats->{$counter} }) or diag "never parked for $counter";
            kill KILL => $pid;
            waitpid $pid, 0;
        };

        $killed_while->(recv_waiters => sub { $srv->recv_wait });
        ok $round_trips->(), 'traffic flows past a dead receiver';
        is $srv->stats->{recv_waiters}, 0, '  and recv_waiters drops back to 0';

        my $cli = $cc->new($p);
        my @queued = map { $cli->send($msg) } 1 .. 2;
        $killed_while->(send_waiters => sub { $cc->new($p)->send_wait($msg) });
        for (@queued) { my (undef, $rid) = $srv->recv; $srv->reply($rid, $msg); $cli->get($_) }
        ok $round_trips->(), 'traffic flows past a dead sender parked for queue room';
        is $srv->stats->{send_waiters}, 0, '  and send_waiters drops back to 0';

        my @held = map { my $id = $cli->send($msg); [$id, ($srv->recv)[1]] } 1 .. 3;
        $killed_while->(slot_waiters => sub { $cc->new($p)->send_wait($msg) });
        for (@held) { $srv->reply($_->[1], $msg); $cli->get($_->[0]) }
        ok $round_trips->(), 'traffic flows past a dead sender parked for a slot';
        is $srv->stats->{slot_waiters}, 0, '  and slot_waiters drops back to 0';
    };
}

# Int queue states a process killed mid-operation leaves: poke them into a fresh file.
{
    my %state = (
        'a producer killed between claiming a queue position and publishing it'
            => sub { my $fh = shift; seek $fh, 128, 0; print {$fh} pack 'Q', 1 },
        'a receiver killed between taking a message and moving the head past it'
            => sub { my $fh = shift; seek $fh, 256, 0; print {$fh} pack 'Q', 4; seek $fh, 128, 0; print {$fh} pack 'Q', 1 },
    );
    for my $what (sort keys %state) {
        subtest "Int: $what does not wedge the queue" => sub {
            my $p = "$dir/hole.shm";
            unlink $p;
            my $srv = Data::ReqRep::Shared::Int->new($p, 4, 4);
            open my $fh, '+<', $p or die $!;
            binmode $fh;
            $state{$what}->($fh);
            close $fh;
            my $cli = Data::ReqRep::Shared::Int::Client->new($p);
            my ($sent, $got) = (0, 0);
            for my $v (1 .. 20) {
                my $id = $cli->send_wait($v, 2) // last;
                $sent++;
                my ($req, $rid) = $srv->recv_wait(2);
                last unless defined $req && $req == $v;
                $srv->reply($rid, $v);
                $got++ if ($cli->get_wait($id, 2) // 0) == $v;
            }
            is $sent, 20, 'every send gets in, lap after lap';
            is $got, 20, '  and every request is received in order and answered';
        };
    }
}

for my $name (qw(Str Int)) {
    my $k = $kind{$name};
    subtest "$name: destroying a client gives up its requests still in flight" => sub {
        my $p = "$dir/destroy_$name.shm";
        my $srv = $k->{server}->($p, 3);
        my $cli = $k->{client}->($p);
        my @ids = map { $cli->send($k->{msg}) } 1 .. 3;
        my (undef, $replied) = $srv->recv;
        $srv->reply($replied, $k->{msg});
        my (undef, $received) = $srv->recv;
        undef $cli;
        my $next = $k->{client}->($p);
        is scalar(grep { defined } map { $next->send($k->{msg}) } 1 .. 3), 3, 'another client gets all three slots at once';
        ok !$srv->reply($received, $k->{msg}), '  and a reply to a request it gave up is refused';
    };
}

# A process that used the channel died and its pid went to another process: poke that state,
# with this process as the newcomer holding the pid.
sub peek32 { my ($fh, $off) = @_; seek $fh, $off, 0; read $fh, my $b, 4; unpack 'L', $b }
sub poke { my ($fh, $off, $fmt, @v) = @_; seek $fh, $off, 0; print {$fh} pack $fmt, @v }
sub reused_pid_file {
    my ($path, $int) = @_;
    unlink $path;
    my $srv = $int ? Data::ReqRep::Shared::Int->new($path, 16, 1) : Data::ReqRep::Shared->new($path, 16, 1, 64);
    open my $fh, '+<', $path or die $!;
    binmode $fh;
    my ($proc_off, $proc_slots) = (peek32($fh, 172), peek32($fh, 176));
    poke($fh, $proc_off + 8 * ($$ & ($proc_slots - 1)), 'LL', $$, 12345);
    return ($srv, $fh);
}

for my $name (qw(Str Int)) {
    subtest "$name: a slot held by a dead process whose pid was reused is recovered" => sub {
        my $p = "$dir/reuse_slot_$name.shm";
        my ($srv, $fh) = reused_pid_file($p, $name eq 'Int');
        my $gen = 3;
        poke($fh, peek32($fh, 44), 'Q', $gen << 32 | ($$ & 0xFFFFFF) << 8 | ($gen & 31) << 3 | 1);
        close $fh;
        my $pid = fork // die $!;
        if (!$pid) { POSIX::_exit(defined $kind{$name}{client}->($p)->send($kind{$name}{msg}) ? 0 : 1) }
        waitpid $pid, 0;
        is $? >> 8, 0, 'another process gets the only slot';
    };
}

subtest 'Str: the process that got the pid of one that died holding the queue mutex can lock it' => sub {
    my $p = "$dir/reuse_mutex.shm";
    my ($srv, $fh) = reused_pid_file($p, 0);
    poke($fh, 192, 'L', 0x8000_0000 | $$);
    close $fh;
    my $t0 = time;
    ok defined Data::ReqRep::Shared::Client->new($p)->send('x'), 'a send gets through';
    cmp_ok time - $t0, '<', 1, '  at once';
};

done_testing;

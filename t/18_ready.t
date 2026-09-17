use strict;
use warnings;
use Test::More;
use File::Temp qw(tempdir);
use IO::Select;
use POSIX ();
use Data::ReqRep::Shared;
use Data::ReqRep::Shared::Client;
use Data::ReqRep::Shared::Int;
use Data::ReqRep::Shared::Int::Client;

my $dir = tempdir(CLEANUP => 1);
my %kind = (
    Str => [sub { Data::ReqRep::Shared->new($_[0], 16, 8, 64) }, sub { Data::ReqRep::Shared::Client->new($_[0]) }, 'x'],
    Int => [sub { Data::ReqRep::Shared::Int->new($_[0], 16, 8) }, sub { Data::ReqRep::Shared::Int::Client->new($_[0]) }, 7],
);

sub readable { IO::Select->new($_[0])->can_read($_[1]) ? 1 : 0 }

for my $name (qw(Str Int)) {
    my ($server, $client, $msg) = @{ $kind{$name} };
    my $p = "$dir/ready_$name.shm";
    my $srv = $server->($p);

    subtest "$name: a client's ready_fd reports its own replies" => sub {
        my $cli = $client->($p);
        my $fd = $cli->ready_fd;
        ok $fd >= 0, 'ready_fd gives a descriptor';
        is $cli->ready_fd, $fd, '  the same one each time';
        my @ids = map { $cli->send($msg) } 1 .. 3;
        ok !readable($fd, 0.2), 'nothing is ready before a reply';
        my $replier = fork // die $!;
        if (!$replier) {
            my $s = $server->($p);
            for (1 .. 3) { my (undef, $id) = $s->recv_wait(2); $s->reply($id, $msg) if $_ < 3; }
            sleep 3;
            POSIX::_exit(0);
        }
        ok readable($fd, 2), 'the descriptor becomes readable when replies land';
        select undef, undef, undef, 0.3;
        is_deeply [sort { $a <=> $b } $cli->ready], [sort { $a <=> $b } @ids[0, 1]], 'ready lists the two replied ids';
        is_deeply [$cli->ready], [], '  once';
        is $cli->get($_), $msg, "  and get takes each" for @ids[0, 1];
        kill KILL => $replier;
        waitpid $replier, 0;
        $cli->cancel($ids[2]);
    };

    subtest "$name: another client is not woken" => sub {
        my $a = $client->($p);
        my $fd_a = $a->ready_fd;
        pipe my $r, my $w or die $!;
        my $b = fork // die $!;
        if (!$b) {
            close $r;
            my $c = $client->($p);
            my $fd = $c->ready_fd;
            my $id = $c->send($msg);
            syswrite $w, pack 'Q', $id;
            my $ok = readable($fd, 3) && grep({ $_ == $id } $c->ready) && defined $c->get($id);
            POSIX::_exit($ok ? 0 : 1);
        }
        close $w;
        sysread $r, my $packed, 8;
        my (undef, $id) = $srv->recv_wait(2);
        is $id, unpack('Q', $packed), 'the other client sent a request';
        ok $srv->reply($id, $msg), '  it is replied to';
        waitpid $b, 0;
        is $? >> 8, 0, '  and the other client is woken for it';
        ok !readable($fd_a, 0.3), 'this client is not';
    };

    subtest "$name: ready skips replies already read, and recovers dropped notifications" => sub {
        my $cli = $client->($p);
        my $fd = $cli->ready_fd;
        my $id = $cli->send($msg);
        my (undef, $rid) = $srv->recv;
        $srv->reply($rid, $msg);
        is $cli->get($id), $msg, 'the reply is read before ready is called';
        is_deeply [$cli->ready], [], '  so ready does not list it';

        my $id2 = $cli->send($msg);
        (undef, $rid) = $srv->recv;
        $srv->reply($rid, $msg);
        readable($fd, 2);
        1 while defined POSIX::read($fd, my $buf, 8);
        is_deeply [$cli->ready], [], 'a notification lost without a trace is not invented';
        open my $fh, '+<', $p or die $!;
        binmode $fh;
        seek $fh, 180, 0; read $fh, my $lost, 4;
        seek $fh, 180, 0; print {$fh} pack 'L', unpack('L', $lost) + 1;
        close $fh;
        is_deeply [$cli->ready], [$id2], 'one counted as dropped is found';
        is $cli->get($id2), $msg, '  and read';
    };

    subtest "$name: a forked child needs its own ready_fd" => sub {
        my $cli = $client->($p);
        my $fd = $cli->ready_fd;
        my $child = fork // die $!;
        if (!$child) {
            my $id = $cli->send($msg);
            my $s = $server->($p);
            my (undef, $rid) = $s->recv_wait(2);
            $s->reply($rid, $msg);
            my $parent_woken = readable($fd, 0.3);
            my $own = $cli->ready_fd;
            my $id2 = $cli->send($msg);
            (undef, $rid) = $s->recv_wait(2);
            $s->reply($rid, $msg);
            my $ok = !$parent_woken && readable($own, 2) && grep({ $_ == $id2 } $cli->ready);
            $cli->get($_) for $id, $id2;
            POSIX::_exit($ok ? 0 : 1);
        }
        waitpid $child, 0;
        is $? >> 8, 0, 'the child is notified only on the descriptor it asked for';
        ok !readable($fd, 0.2), '  and the parent is not woken for its replies';
    };

    subtest "$name: ready recovers all replies when count exceeds 4096 on dropped notifications" => sub {
        my $p_large = "$dir/ready_large_$name.shm";
        my $s_large = $name eq 'Str'
            ? Data::ReqRep::Shared->new($p_large, 5000, 5000, 64)
            : Data::ReqRep::Shared::Int->new($p_large, 5000, 5000);
        my $c_large = $name eq 'Str'
            ? Data::ReqRep::Shared::Client->new($p_large)
            : Data::ReqRep::Shared::Int::Client->new($p_large);
        my $fd = $c_large->ready_fd;
        my $cnt = 4500;
        my @sent = map { $c_large->send($msg) } 1 .. $cnt;
        for (1 .. $cnt) {
            my (undef, $rid) = $s_large->recv;
            $s_large->reply($rid, $msg);
        }
        my @r1 = $c_large->ready;
        ok readable($fd, 1), 'the descriptor stays readable while ids are left over';
        my @r2 = $c_large->ready;
        is scalar(@r1) + scalar(@r2), $cnt, "all $cnt replies retrieved without loss";
        ok !readable($fd, 0.2), '  and goes quiet once they are all taken';
        is_deeply [sort { $a <=> $b } (@r1, @r2)], [sort { $a <=> $b } @sent], "  matching all sent ids";
        $c_large->get($_) for @sent;
        unlink $p_large;
    };
}

done_testing;

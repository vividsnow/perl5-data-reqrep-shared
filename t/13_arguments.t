use strict;
use warnings;
use utf8;
use Test::More;
use Config;
use File::Temp qw(tempdir);
use POSIX ();
use Data::ReqRep::Shared;
use Data::ReqRep::Shared::Client;
use Data::ReqRep::Shared::Int;
use Data::ReqRep::Shared::Int::Client;

my $dir = tempdir(CLEANUP => 1);

subtest 'a path with a NUL byte is refused, never cut short' => sub {
    my $victim = "$dir/important.db";
    open my $fh, '>', $victim or die $!;
    close $fh;
    ok !eval { Data::ReqRep::Shared->unlink("$victim\0.shm"); 1 }, 'class unlink refuses it';
    ok -e $victim, '  and the file named by the part before the NUL survives';
    ok !eval { Data::ReqRep::Shared::Int->unlink("$victim\0.shm"); 1 }, 'Int class unlink too';
    ok -e $victim, '  and it still survives';

    ok !eval { Data::ReqRep::Shared->new("$dir/a.shm\0x", 4, 2, 64); 1 }, 'new refuses it';
    ok !-e "$dir/a.shm", '  and creates nothing';
    ok !eval { Data::ReqRep::Shared::Int->new("$dir/b.shm\0x", 4, 2); 1 }, 'Int new too';
    Data::ReqRep::Shared->new("$dir/c.shm", 4, 2, 64);
    ok !eval { Data::ReqRep::Shared::Client->new("$dir/c.shm\0x"); 1 }, 'Client new too';
    Data::ReqRep::Shared::Int->new("$dir/d.shm", 4, 2);
    ok !eval { Data::ReqRep::Shared::Int::Client->new("$dir/d.shm\0x"); 1 }, 'Int::Client new too';
};

{ package Сервер; our @ISA = ('Data::ReqRep::Shared') }

subtest 'constructors bless into the class they were called on' => sub {
    my $s = Сервер->new(undef, 4, 2, 64);
    is ref $s, 'Сервер', 'a UTF-8 class name is kept';
    is eval { $s->capacity }, 4, '  and its methods resolve';
    my $m = Data::ReqRep::Shared->new_memfd('m', 4, 2, 64);
    my $again = $m->new_memfd('n', 4, 2, 64);
    is ref $again, 'Data::ReqRep::Shared', 'called on an object, a constructor uses its class';
};

subtest 'file descriptor arguments must be descriptors' => sub {
    my $s  = Data::ReqRep::Shared->new_memfd('fd', 4, 2, 64);
    my $evsrv = Data::ReqRep::Shared->new(undef, 4, 2, 64);
    my $ev    = $evsrv->eventfd;
    for my $bad ([undef, 'undef'], [2**32 + 1, '2**32+1'], [-1, '-1'], ['3x', 'a non-number'], [\*STDOUT, 'a glob ref']) {
        ok !eval { $s->eventfd_set($bad->[0]); 1 }, "eventfd_set refuses $bad->[1]";
    }
    ok !eval { $s->eventfd_set($s->memfd); 1 }, 'eventfd_set refuses a descriptor that is not an eventfd';
    like $@, qr/not an eventfd/, '  and says so';
    ok eval { $s->eventfd_set($ev); 1 }, 'a real eventfd is accepted' or diag $@;
    ok !eval { Data::ReqRep::Shared::Client->new_from_fd($s->memfd + 2**32); 1 },
        'new_from_fd refuses a value that only truncates to an open descriptor';
};

subtest 'drain takes a count wider than 32 bits' => sub {
    my $s = Data::ReqRep::Shared->new_memfd('dr', 8, 8, 64);
    my $c = Data::ReqRep::Shared::Client->new_from_fd($s->memfd);
    $c->send("m$_") for 1 .. 3;
    is scalar(my @got = $s->drain(2**32)), 6, 'drain(2**32) takes all three messages';
};

subtest 'a segment larger than the address space croaks' => sub {
    plan skip_all => 'only a 32-bit address space can be exceeded cheaply' unless $Config{ptrsize} < 8;
    ok !eval { Data::ReqRep::Shared->new(undef, 16, 4097, 1 << 20); 1 }, 'Str';
    like $@, qr/layout overflow/, '  with the reason';
    ok !eval { Data::ReqRep::Shared::Int->new(undef, 16, 67108865); 1 }, 'Int';
    like $@, qr/layout overflow/, '  with the reason';
};

subtest 'every class reports the same stats keys; only Str has an arena' => sub {
    my $s = Data::ReqRep::Shared->new_memfd('st', 4, 2, 64);
    my $i = Data::ReqRep::Shared::Int->new_memfd('it', 4, 2);
    my %keys = map { ref($_) => join ' ', sort keys %{ $_->stats } }
        $s, Data::ReqRep::Shared::Client->new_from_fd($s->memfd),
        $i, Data::ReqRep::Shared::Int::Client->new_from_fd($i->memfd);
    is $keys{'Data::ReqRep::Shared::Client'}, $keys{'Data::ReqRep::Shared'}, 'Str client matches Str server';
    is $keys{'Data::ReqRep::Shared::Int'}, join(' ', grep { !/^arena_/ } split ' ', $keys{'Data::ReqRep::Shared'}),
        'Int server matches Str without arena_*';
    is $keys{'Data::ReqRep::Shared::Int::Client'}, $keys{'Data::ReqRep::Shared::Int'}, 'Int client matches Int server';
};

subtest 'resp_slots must fit a signed 32-bit index' => sub {
    ok !eval { Data::ReqRep::Shared->new(undef, 16, 2**31, 1); 1 }, 'Str: 2**31 slots croak before mapping anything';
    like $@, qr/layout overflow/, '  with the reason';
    ok !eval { Data::ReqRep::Shared::Int->new(undef, 16, 2**31); 1 }, 'Int too';
};

for my $v (['Str', 'Data::ReqRep::Shared', 'Data::ReqRep::Shared::Client', [64], 'ping', 'pong'],
           ['Int', 'Data::ReqRep::Shared::Int', 'Data::ReqRep::Shared::Int::Client', [], 41, 42]) {
    my ($name, $sc, $cc, $extra, $q, $a) = @$v;
    subtest "$name: an anonymous channel serves a forked client" => sub {
        my $srv = $sc->new(undef, 4, 2, @$extra);
        cmp_ok $srv->memfd, '>=', 0, 'it has a descriptor to attach by';
        pipe my $r, my $w or die $!;
        my $pid = fork // die $!;
        if (!$pid) {
            close $r;
            my $got = eval { $cc->new_from_fd($srv->memfd)->req_wait($q, 5) };
            syswrite $w, defined $got ? $got : "failed: $@";
            POSIX::_exit(0);
        }
        close $w;
        my ($req, $id) = $srv->recv_wait(5);
        $srv->reply($id, $a) if defined $id;
        waitpid $pid, 0;
        is do { local $/; <$r> }, $a, 'the child gets its reply';
    };
}

subtest "an id's reply is taken by whichever process reads it" => sub {
    my $srv = Data::ReqRep::Shared->new_memfd('fk', 4, 2, 64);
    my $cli = Data::ReqRep::Shared::Client->new_from_fd($srv->memfd);
    my $id = $cli->send('q');
    my (undef, $rid) = $srv->recv;
    $srv->reply($rid, 'r');
    pipe my $r, my $w or die $!;
    my $pid = fork // die $!;
    if (!$pid) { close $r; syswrite $w, $cli->get($id) // 'undef'; POSIX::_exit(0) }
    close $w;
    waitpid $pid, 0;
    is do { local $/; <$r> }, 'r', 'a forked child reading the id gets the reply';
    is $cli->get($id), undef, '  and the parent then gets undef';
    is $cli->pending, 0, '  with the slot freed';
};

subtest 'a generation that wraps skips 0, so no request id is ever 0' => sub {
    for my $v (['Str', 'Data::ReqRep::Shared', 'Data::ReqRep::Shared::Client', [64]],
               ['Int', 'Data::ReqRep::Shared::Int', 'Data::ReqRep::Shared::Int::Client', []]) {
        my ($name, $sc, $cc, $extra) = @$v;
        my $p = "$dir/genwrap$name.shm";
        $sc->new($p, 4, 1, @$extra);
        open my $fh, '+<', $p or die $!;
        binmode $fh;
        seek $fh, 44, 0; read $fh, my $off, 4; $off = unpack 'L', $off;
        seek $fh, $off, 0; print {$fh} pack 'Q', 0xFFFF_FFFF << 32 | 31 << 3;
        close $fh;
        my $id = $cc->new($p)->send($name eq 'Str' ? 'x' : 1);
        ok $id, "$name: the send after generation 2**32-1 gets a non-zero id";
        is $id >> 32, 1, '  with generation 1';
    }
};

done_testing;

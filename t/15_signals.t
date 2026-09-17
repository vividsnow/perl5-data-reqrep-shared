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

# Each call runs in a child that gets SIGUSR1 after 0.3 s.

my $dir = tempdir(CLEANUP => 1);
my $n   = 0;

sub blocked {
    my (%a) = @_;
    my $p = "$dir/s" . ++$n . '.shm';
    my $srv = $a{server}->($p);
    pipe my $r, my $w or die $!;
    my $pid = fork // die $!;
    if (!$pid) {
        close $r;
        my $ran = 0;
        $SIG{USR1} = $a{dies} ? sub { die "signalled\n" } : sub { $ran++ };
        my $t0  = time;
        my $out = eval { $a{call}->($p) };
        my $err = $@ =~ /^signalled/ ? 'signalled' : $@ ? "other: $@" : 'none';
        $out = $a{after}->() if $a{after};
        syswrite $w, sprintf "out=%s secs=%.1f ran=%d err=%s\n", $out // 'undef', time - $t0, $ran, $err;
        POSIX::_exit(0);
    }
    close $w;
    sleep 0.3;
    kill USR1 => $pid;
    $a{then}->($srv, $p) if $a{then};
    my $rin = '';
    vec($rin, fileno $r, 1) = 1;
    my $line = select(my $rout = $rin, undef, undef, 8) ? <$r> : "no report within 8s\n";
    kill KILL => $pid;
    waitpid $pid, 0;
    chomp $line;
    return $line;
}

my %kind = (
    Str => { server => sub { Data::ReqRep::Shared->new($_[0], 16, 1, 64) },
             client => sub { Data::ReqRep::Shared::Client->new($_[0]) }, msg => 'x' },
    Int => { server => sub { Data::ReqRep::Shared::Int->new($_[0], 16, 1) },
             client => sub { Data::ReqRep::Shared::Int::Client->new($_[0]) }, msg => 7 },
);

for my $name (qw(Str Int)) {
    my $k = $kind{$name};
    my $srv_class = ref $k->{server}->("$dir/probe_$name.shm");
    my %calls = (
        'recv_wait'     => sub { $srv_class->new($_[0], 16, 1, $name eq 'Str' ? 64 : ())->recv_wait },
        'recv_wait(30)' => sub { $srv_class->new($_[0], 16, 1, $name eq 'Str' ? 64 : ())->recv_wait(30) },
        'send_wait'     => sub { my $c = $k->{client}->($_[0]); $c->send($k->{msg}); $c->send_wait($k->{msg}) },
        'get_wait'      => sub { my $c = $k->{client}->($_[0]); $c->get_wait($c->send($k->{msg})) },
        'req'           => sub { $k->{client}->($_[0])->req($k->{msg}) },
        'req_wait(30)'  => sub { $k->{client}->($_[0])->req_wait($k->{msg}, 30) },
    );
    $calls{'recv_wait_multi(4)'} = sub { Data::ReqRep::Shared->new($_[0], 16, 1, 64)->recv_wait_multi(4) }
        if $name eq 'Str';

    for my $call (sort keys %calls) {
        my $line = blocked(server => $k->{server}, call => $calls{$call}, dies => 1);
        like $line, qr/secs=0\.\d .*err=signalled/, "$name $call: a handler that dies ends the wait"
            or diag $line;
    }

    my $c;
    my $line = blocked(
        server => $k->{server}, dies => 1,
        call   => sub { $c = $k->{client}->($_[0]); $c->req($k->{msg}) },
        after  => sub { 'pending=' . $c->pending });
    like $line, qr/out=pending=0 .*err=signalled/, "$name req: a handler that dies leaves no slot behind"
        or diag $line;

    $line = blocked(
        server => $k->{server},
        call   => sub { $k->{client}->($_[0])->req($k->{msg}) },
        then   => sub {
            my ($srv) = @_;
            sleep 0.5;
            my ($q, $id) = $srv->recv_wait(5);
            $srv->reply($id, $name eq 'Str' ? "re:$q" : $q + 1);
        });
    my $want = $name eq 'Str' ? 're:x' : 8;
    like $line, qr/out=\Q$want\E .*ran=1 err=none/, "$name req: a handler that returns lets it wait for the reply"
        or diag $line;
}

my $line = blocked(
    server => $kind{Str}{server},
    call   => sub { my @m = Data::ReqRep::Shared->new($_[0], 16, 1, 64)->recv_wait; $m[0] },
    then   => sub { sleep 0.5; Data::ReqRep::Shared::Client->new($_[1])->send('hello') });
like $line, qr/out=hello .*ran=1 err=none/, 'Str recv_wait: a handler that returns lets it wait for the message'
    or diag $line;

# The call sits directly in eval, where unwinding frees temporaries before it
# runs the scope's destructors. The child checks its slot while still alive.
for my $name (qw(Str Int)) {
    my $k = $kind{$name};
    my $cc = ref $k->{client}->(do { my $p = "$dir/cls_$name.shm"; $k->{server}->($p); $p });
    my %drop = (
        'undef $c; die'        => sub { my $c = $cc->new($_[0]); local $SIG{ALRM} = sub { undef $c; die "alarm\n" }; eval { $c->req($k->{msg}) } },
        'delete $h{c}; die'    => sub { my %h = (c => $cc->new($_[0])); local $SIG{ALRM} = sub { delete $h{c}; die "alarm\n" }; eval { $h{c}->req($k->{msg}) } },
        'reconnect and return' => sub { my $p = shift; my $c = $cc->new($p); local $SIG{ALRM} = sub { $c = $cc->new($p) }; eval { $c->req_wait($k->{msg}, 1) }; die "croaked: $@" if $@ },
    );
    for my $case (sort keys %drop) {
        my $p = "$dir/drop" . ++$n . '.shm';
        my $srv = $k->{server}->($p);
        my $pid = fork // die $!;
        if (!$pid) {
            Time::HiRes::ualarm(300_000);
            eval { $drop{$case}->($p); 1 } or POSIX::_exit(3);
            POSIX::_exit(defined $cc->new($p)->send($k->{msg}) ? 0 : 2);
        }
        waitpid $pid, 0;
        is $?, 0, "$name: a handler that does '$case' during a request leaves no crash and no held slot";
    }
}

done_testing;

package Data::ReqRep::Shared;
use strict;
use warnings;
our $VERSION = '0.09';

require XSLoader;
XSLoader::load('Data::ReqRep::Shared', $VERSION);

# ithreads: blessed shared-memory handles must never be cloned into a
# child thread -- the clone would double-free the handle on thread exit.
{ no strict 'refs'; *{"${_}::CLONE_SKIP"} = sub { 1 } for qw(
  Data::ReqRep::Shared
  Data::ReqRep::Shared::Client
  Data::ReqRep::Shared::Int
  Data::ReqRep::Shared::Int::Client
); }

1;

__END__

=encoding utf-8

=head1 NAME

Data::ReqRep::Shared - High-performance shared-memory request/response IPC for Linux

=head1 SYNOPSIS

    use Data::ReqRep::Shared;

    # Server: create channel
    my $srv = Data::ReqRep::Shared->new('/tmp/rr.shm', 1024, 64, 4096);
    #   path, req_capacity, resp_slots, resp_data_max

    # Server loop
    while (my ($req, $id) = $srv->recv_wait) {
        $srv->reply($id, process($req));
    }

    # Client: open existing channel
    my $cli = Data::ReqRep::Shared::Client->new('/tmp/rr.shm');

    # Synchronous
    my $resp = $cli->req("hello");

    # With timeout (single deadline covers send + wait)
    my $resp = $cli->req_wait("hello", 5.0);

    # Asynchronous (multiple in-flight)
    my $id1 = $cli->send("req1");
    my $id2 = $cli->send("req2");
    my $r1  = $cli->get_wait($id1);
    my $r2  = $cli->get_wait($id2);

    # Integer variant (lock-free)
    use Data::ReqRep::Shared::Int;
    my $srv = Data::ReqRep::Shared::Int->new($path, 1024, 64);
    my $cli = Data::ReqRep::Shared::Int::Client->new($path);
    my $resp = $cli->req(42);

=head1 DESCRIPTION

Shared-memory request/response channel for interprocess communication
on Linux. Multiple clients send requests, multiple workers process
them, responses are routed back to the correct requester. All through
a single shared-memory file -- no broker process, no socket pairs per
connection.

B<Linux-only>. Requires a Perl with 64-bit integers.

=head2 Architecture

=over

=item * B<Request queue> -- bounded MPMC ring buffer. Str variant uses
a futex mutex with circular arena for variable-length data. Int
variant uses a lock-free MPMC queue.

=item * B<Response slots> -- fixed pool with per-slot futex for targeted
wakeup and a generation counter for ABA-safe cancel/recycle.

=back

Flow: client acquires a response slot, pushes a request (carrying the
slot ID), server pops the request, writes the response to that slot,
client reads it and releases the slot.

=head2 Variants

=over

=item B<Str> -- C<Data::ReqRep::Shared> / C<Data::ReqRep::Shared::Client>

Variable-length byte string requests and responses. Mutex-protected
request queue with circular arena. Supports UTF-8 flag preservation.

    my $srv = Data::ReqRep::Shared->new($path, $cap, $slots, $resp_size);
    my $srv = Data::ReqRep::Shared->new($path, $cap, $slots, $resp_size, $arena);

=item B<Int> -- C<Data::ReqRep::Shared::Int> / C<Data::ReqRep::Shared::Int::Client>

Single int64 request and response values: numbers outside that range,
fractions and non-numeric strings are converted the way Perl converts to an
integer, so check them first. Lock-free MPMC request queue. No arena,
no mutex on the request path.

    my $srv = Data::ReqRep::Shared::Int->new($path, $cap, $slots);

=back

Both variants share the same response slot infrastructure, the same
generation-counter ABA protection, the same eventfd integration, and
the same response-slot crash recovery. Their request queues differ: see
L</CRASH SAFETY>.

=head2 Constructors

B<Server> (creates or opens the channel):

    ->new($path, ...)             # file-backed
    ->new(undef, ...)             # anonymous (fork-inherited)
    ->new_memfd($name, ...)       # memfd (fd-passing or fork)
    ->new_from_fd($fd)            # open from memfd fd

An anonymous channel is an unnamed memfd: a forked child attaches with
C<< Client->new_from_fd($srv->memfd) >>.

B<Client> (opens existing channel):

    ->new($path)
    ->new_from_fd($fd)

Constructor arguments for Str: C<$path, $req_cap, $resp_slots, $resp_size [,
$arena [, $mode]]>. For Int: C<$path, $req_cap, $resp_slots [, $mode]>.
C<$mode> is the octal backing-file permission (default C<0600>); see
L</SECURITY>. The descriptor you pass is duplicated (C<F_DUPFD_CLOEXEC>), so
it stays yours to close and closing it does not disturb the handle. C<new> on
a file that already holds a channel attaches to it with the sizes it was
created with. The sizes you pass are not compared with those, but they must
still be valid.

A handle cannot be copied. A copy made by Storable or Clone croaks when used,
and a new thread gets no usable handles; open the channel again instead.

=head2 Server API

    my ($data, $id) = $srv->recv;              # non-blocking
    my ($data, $id) = $srv->recv_wait;         # blocking
    my ($data, $id) = $srv->recv_wait($secs);  # with timeout

Returns C<($request_data, $id)> or empty list. For Int, C<$data> is
an integer. Call them in list context: in scalar context they return the id.

    my $ok = $srv->reply($id, $response);

Writes response and wakes the client. Returns false if the slot was
cancelled or recycled (generation mismatch), or if C<$id> names no slot.
Only the process that received the request can reply to it: a reply from any
other, such as a child forked after C<recv>, returns false.

B<Batch> (Str only):

    my @pairs = $srv->recv_multi($n);          # up to $n under one lock
    my @pairs = $srv->recv_wait_multi($n, $timeout);
    my @pairs = $srv->drain;
    my @pairs = $srv->drain($max);

Returns flat list C<($data1, $id1, $data2, $id2, ...)>.

B<Management>:

    $srv->clear;       $srv->sync;        $srv->unlink;
    $srv->size;        $srv->capacity;    $srv->is_empty;
    $srv->resp_slots;  $srv->resp_size;   $srv->stats;
    $srv->path;        $srv->memfd;

C<path> is kept as given, so a relative one is looked up again from the
current directory: C<unlink> after a C<chdir> misses the file, and treats a
missing file as already removed. In C<stats>, C<recv_empty> and C<recoveries>
are 32-bit and wrap; the other counters are 64-bit.

B<eventfd> (see L</Event Loop Integration>):

    $srv->eventfd;             $srv->eventfd_set($fd);
    $srv->eventfd_consume;     $srv->notify;
    $srv->fileno;              # current request eventfd (-1 if none)
    $srv->reply_eventfd;       $srv->reply_eventfd_set($fd);
    $srv->reply_eventfd_consume;  $srv->reply_notify;
    $srv->reply_fileno;        # current reply eventfd (-1 if none)

=head2 Client API

B<Synchronous>:

    my $resp = $cli->req($data);                # infinite wait
    my $resp = $cli->req_wait($data, $secs);    # single deadline

Both return undef if no reply arrives: C<req_wait> on its timeout, and either
within two seconds of the server that received the request dying before its
reply is complete.

Perl signal handlers, C<alarm> included, run while a call waits for a request,
a reply, a free slot, queue room or the Str queue mutex. A handler that dies
ends the call, cancelling a request still waiting for its reply; one that
returns lets the call carry on waiting. A constructor waiting for the file lock
runs them only once that wait ends.

A timeout of 0 never waits for a request, a reply, a slot or room. A Str call
still waits for the queue mutex, for two seconds at most when the process
holding it is stopped. A negative timeout, or NaN, waits for ever like
no timeout at all, so clamp a computed remaining time at 0. C<req_wait> reads
an undef timeout as 0; the other waits read it as no timeout.

B<Asynchronous>:

    my $id   = $cli->send($data);               # non-blocking
    my $id   = $cli->send_wait($data, $secs);   # blocking
    my $resp = $cli->get($id);                   # non-blocking
    my $resp = $cli->get_wait($id, $secs);       # blocking
    $cli->cancel($id);                           # abandon request

C<cancel> releases the slot only if the reply hasn't arrived yet. If it has,
cancel is a no-op -- call C<get()> to drain, or the slot stays held until the
client exits. A reply still being copied in does not delay C<cancel>: the
responder frees the slot when its copy finishes. C<req_wait> does the
cancel-and-drain for you. A C<get_wait> that times out leaves the request in
flight: wait again or C<cancel> it, or its slot stays held until the client
exits.

Destroying a client cancels its requests still in flight, freeing their slots;
a forked child destroying its copy of the parent's client leaves them alone.

A reply is taken by whichever process reads it with the request's id: a forked
child that calls C<get> with its parent's id receives the reply and frees the
slot, and the parent then gets undef.

B<Convenience> (Str only):

    my $id = $cli->send_notify($data);          # send + eventfd signal
    my $id = $cli->send_wait_notify($data);

B<Status>:

    $cli->pending;     $cli->size;       $cli->capacity;
    $cli->is_empty;    $cli->resp_slots; $cli->resp_size;
    $cli->stats;       $cli->path;       $cli->memfd;

B<eventfd> (see L</Event Loop Integration>):

    $cli->eventfd;             $cli->eventfd_set($fd);
    $cli->eventfd_consume;     $cli->fileno;
    $cli->notify;              # signal request eventfd
    $cli->req_eventfd_set($fd);  $cli->req_fileno;
    $cli->ready_fd;            $cli->ready;   # this client's own replies

=head2 Event Loop Integration

Two eventfds for bidirectional notification. Both are opt-in --
C<send>/C<reply> do not signal automatically.

    # Request notification (client -> server)
    my $req_fd = $srv->eventfd;     # create
    $srv->eventfd_consume;          # drain in callback
    $cli->notify;                   # signal (or send_notify)
    $cli->req_eventfd_set($fd);     # set inherited fd

    # Reply notification (server -> client)
    my $rep_fd = $srv->reply_eventfd;
    $srv->reply_notify;             # signal after reply
    $cli->eventfd;                  # create (maps to reply fd)
    $cli->eventfd_consume;          # drain in callback
    $cli->eventfd_set($fd);         # set inherited fd

The C<*_eventfd_set> methods duplicate the descriptor, as C<new_from_fd> does:
the one you pass stays yours to close. They croak on a descriptor that is not
open or is not an eventfd, since C<notify> writes into it. One you consume
through must be non-blocking (C<EFD_NONBLOCK>, as the module's own are), or
C<eventfd_consume> blocks when another process has already drained it.

Each eventfd is one counter for the whole channel, not one per client: an
C<eventfd_consume> in one client takes the notifications meant for all of
them. With more than one client, use C<ready_fd> instead, check every
outstanding id after each wakeup, or wait with C<get_wait> and a timeout.

B<Per-client reply notification>:

    my $fd = $cli->ready_fd;        # this client's own descriptor
    my $id = $cli->send($data);     # replies to requests sent from now on wake it
    my $w  = EV::io $fd, EV::READ, sub {
        handle($_, $cli->get($_)) for $cli->ready;
    };

C<ready_fd> gives the client a descriptor of its own that becomes readable
when a reply to one of its requests is ready. Clients sharing a channel then
neither wake each other nor take each other's notifications, and no descriptor
has to pass between processes: C<reply> notifies the client without being
asked. It covers requests the client sends after the call, from the same
process; a forked child calls C<ready_fd> again for its own. C<ready> returns
the ids whose replies are ready and still unread, each once, a few thousand at
a time; while any are left over the descriptor stays readable.

The descriptor is an abstract Unix datagram socket, so clients and servers must
share a network namespace. A notification that finds the client's queue full
(C<net.unix.max_dgram_qlen>) is not lost: the next C<ready> looks through the
client's slots.

For cross-process use, create both eventfds B<before> C<fork()> so
child inherits the fds:

    my $srv = Data::ReqRep::Shared->new($path, 1024, 64, 4096);
    my $req_fd = $srv->eventfd;
    my $rep_fd = $srv->reply_eventfd;

    if (fork() == 0) {
        my $cli = Data::ReqRep::Shared::Client->new($path);
        $cli->req_eventfd_set($req_fd);
        $cli->eventfd_set($rep_fd);
        $cli->send_notify($data);       # wakes server
        # EV::io $rep_fd for reply ...
        exit;
    }

    # parent = server
    my $w = EV::io $req_fd, EV::READ, sub {
        $srv->eventfd_consume;
        while (my ($req, $id) = $srv->recv) {
            $srv->reply($id, process($req));
        }
        $srv->reply_notify;
    };

=head2 Crash Safety

=over

=item * B<Stale mutex> -- if a process dies holding the request queue
mutex, other processes detect it via PID tracking and recover within
2 seconds.

=item * B<Stale response slots> -- a slot held by a client that died, or
by a server that died before replying, is reclaimed automatically
during the next slot acquisition scan. A C<send_wait> or C<req> waiting
for a slot looks again every two seconds, so it notices when the
holders are gone.

=item * B<ABA protection> -- response slot IDs carry a generation
counter. A cancelled-and-reacquired slot has a different generation,
so stale C<reply>/C<get>/C<cancel> calls are safely rejected.

=back

=head2 Tuning

=over

=item C<req_cap> -- request queue capacity (power of 2). Higher for
bursty workloads (1024-4096), lower for steady-state (64-256).
Memory: 24 bytes/slot + arena (Str) or 24 bytes/slot (Int).

=item C<resp_slots> -- max concurrent in-flight requests across all
clients. One slot per outstanding async request. For synchronous
C<req()>, one per client suffices. Memory: 64 bytes/slot (Int) or
(40 + C<resp_size> rounded up to 64) bytes/slot (Str).

=item C<resp_size> -- max response payload bytes (Str only). Fixed per slot. A
longer reply croaks in the responder and its request gets no reply, so size it
for the largest reply you send, not a typical one.

=item C<arena> -- request data arena bytes (Str only, default C<req_cap *
256>). Increase for large requests. Monitor C<arena_used> in C<stats()>.
A waiting sender that does not fit holds the room it needs, if no other waiting
sender needs more: smaller requests use only what is left, so a large request
is not kept out by a stream of small ones. A sender stopped while it waits
keeps that room held until it continues or gives up.

=back

=head2 Benchmarks

Linux x86_64. The single-process rows come from C<bench/bench_int.pl> and
C<bench/bench.pl>, the cross-process rows from C<bench/vs.pl 50000>; run them
with C<perl -Mblib>.

    SINGLE-PROCESS ECHO (200K iterations)
    ReqRep::Int (lock-free)    1.8M req/s
    ReqRep::Str (12B, mutex)   1.2M req/s
    ReqRep::Str batch (100x)   1.4M req/s

    CROSS-PROCESS ECHO (50K iterations, 12B payload)
    Pipe pair (1:1)            240K req/s
    Unix socketpair (1:1)      222K req/s
    ReqRep::Int                202K req/s  *
    ReqRep::Str                177K req/s  *
    IPC::Msg (SysV)            165K req/s
    TCP loopback               115K req/s
    MCE::Channel                96K req/s
    Socketpair via broker       82K req/s
    Forks::Queue (Shmem)         5K req/s

C<*> = MPMC with per-request reply routing. Pipes and sockets are
faster for simple 1:1 echo but require dedicated fd pairs per
client-worker connection and cannot do MPMC without a broker (which
halves throughput).


=head1 CRASH SAFETY

Response slots are recovered from dead owners, and the Str request queue
recovers a mutex held by a dead process. The Int request queue survives its
users being killed too. A receiver killed while taking a message is moved past
by the next sender or receiver. A sender killed between claiming a queue
position and publishing its message holds up the queue only until a receiver
finds that no live process holds the claim and skips the position; that
message is lost. A sender stopped there holds up the queue until it continues.

A process killed while it waits stays counted among the waiters (the
C<*_waiters> in C<stats>) until a few wakes in a row find nobody to wake; it
then drops out of the count.

A slot's generation counter is 32-bit. It guards against a stale id being
honoured after the slot is recycled, which it does for any realistic run;
after 2^32 re-acquisitions of the same slot an ancient id would compare equal
again.

Recovery tells a dead process from a live one by its PID and its start time,
which a channel records for up to 1024 processes that use it at once, so a PID
reused by another process does not keep what the dead one held. A process
beyond those 1024, or one whose F</proc/PID/stat> cannot be read, is known by
its PID alone: if that PID is reused before recovery runs, it is taken for the
process that died.

Every change to a response slot is one compare-and-swap on a word holding its
generation, its state and the pid of the process responsible for it: the
owner, or the server once it has received the request. A stale id or a stale
reading therefore never moves a slot, and recovery acts only on a named
process that is dead, never on elapsed time, so a process stopped by SIGSTOP,
a debugger or a paused container is not taken for dead however long it stays
stopped. When the server dies before its reply is complete, the client's
C<get_wait> or C<req> gives up within two seconds and frees the slot. One
exception: a server that took a request off the queue but has not yet marked it
received, a matter of a few instructions, is taken for dead once the request
stays unmarked for two two-second checks in a row, since it cannot be named.

C<clear> may run while clients and servers are working. Requests it discards
get undef, and a reply still being written is left to its responder, which
frees the slot when done.

An interrupted create is recovered too. A creator killed after the backing
file is sized but before its header is committed leaves a full-size, all-zero
file. C<new> re-initializes such a file automatically, but only when it is
exactly the size the requested geometry needs, is owned by your effective uid,
and is still entirely zero -- a file holding data is never re-initialized. If
the creator got as far as writing part of the header, the file cannot be told
apart from a corrupt one and C<new> croaks with C<incomplete reqrep file left
by an interrupted create; remove it and retry>. A file left behind by an
interrupted create never held data, so removing it is safe -- but a file whose
header was corrupted after the fact reaches the same croak, so confirm it is
an abandoned create before deleting anything you care about.

=head1 CONTAINERS

Stale-slot and stale-mutex recovery identify peers by PID, and a PID only means
something inside one PID namespace. A peer attaching from another namespace
would read live processes as dead -- taking their slots and misdelivering
replies -- and unrelated local processes as alive, never recovering a real
casualty. None of that is detectable after the fact, so C<new> refuses it: the
header records the creating process's PID namespace and the current boot id,
and attaching from anywhere else croaks.

B<All peers must therefore share a PID namespace> -- C<docker run
--pid=container:NAME>, or a Kubernetes pod with C<shareProcessNamespace: true>.
Sharing only the filesystem or the IPC namespace is not enough. The same check
rejects a file left over from a previous boot, whose recorded PIDs now name
unrelated processes.

Set C<DATA_REQREP_SHARED_UNSAFE_PIDNS=1> (any value but C<0>, C<false>, C<no>
or C<off>) to attach anyway. Only do that if you do not depend on recovery --
for example a fixed set of peers that never die mid-request -- because the
failure mode it re-enables is silent corruption.

For sharing across containers without a shared filesystem, create the segment
with C<new_memfd> and pass the descriptor over a unix socket with
C<SCM_RIGHTS>; a memfd crosses namespaces natively. If you use a file, note
that a container's default C</dev/shm> is often only 64 MB. C<new> reserves
the whole segment when it creates one and croaks if the filesystem cannot hold
it; a sparse segment would instead kill a process with SIGBUS at the first
write the filesystem could not back. Set C<DATA_REQREP_SHARED_SPARSE=1> for
the old sparse file when a segment is far larger than it will ever fill. Under
a user namespace, C<new>'s ownership checks compare uids I<as mapped in the
caller's namespace>, so peers need a common id mapping.

=head1 SEE ALSO

L<Data::Buffer::Shared> - typed shared array

L<Data::HashMap::Shared> - concurrent hash table

L<Data::Queue::Shared> - FIFO queue

L<Data::PubSub::Shared> - publish-subscribe ring

L<Data::Sync::Shared> - synchronization primitives

L<Data::Pool::Shared> - fixed-size object pool

L<Data::Stack::Shared> - LIFO stack

L<Data::Deque::Shared> - double-ended queue

L<Data::Log::Shared> - append-only log (WAL)

L<Data::Heap::Shared> - priority queue

L<Data::Graph::Shared> - directed weighted graph

L<Data::BitSet::Shared> - shared bitset (lock-free per-bit ops)

L<Data::RingBuffer::Shared> - fixed-size overwriting ring buffer

=head1 SECURITY

Backing files are created with mode C<0600> (owner-only) by default, so only
the creating user can open and attach them. To share a backing file across
users, pass an explicit octal file mode such as C<0660> as the final C<$mode>
argument to C<new> -- for Str after the optional C<$arena> (C<< new($path,
$req_cap, $resp_slots, $resp_size, $arena, 0660) >>), for Int as the fourth
argument (C<< new($path, $req_cap, $resp_slots, 0660) >>); the mode is applied
when the file is created, and when a file left behind by an interrupted create
is re-initialized (see L</CRASH SAFETY>); a file already in use keeps its own
permissions. The file is opened with C<O_NOFOLLOW>, so a symlink planted at
the path is refused, and created with C<O_EXCL>; the on-disk header is
validated when the file is attached. Attaching refuses a world-writable file
owned by another user; share with a group mode such as C<0660> instead. Any
process that can open the file can hold its lock, so C<new> waits for the lock
for at most 10 seconds and then croaks. Any process you grant write access to a
shared mapping is trusted not to corrupt its contents while other processes
are using it.

=head1 AUTHOR

vividsnow

=head1 LICENSE

This is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut

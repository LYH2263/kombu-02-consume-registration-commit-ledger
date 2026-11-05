"""Acceptance tests for the channel-bound consumption registration &
cancellation commit ledger (``kombu.consume_ledger``).

Every scenario here runs on ONE channel: nested scopes, Consumer and
SimpleBase lifecycles and the virtual / redis / SQS ``basic_cancel``
completion planes all share the same ledger.  Assertions follow the causal
sequence of commit records instead of poking at ``_active_tags`` length or
the ``_consuming`` boolean.
"""
from __future__ import annotations

from queue import Empty

import pytest

from kombu import Connection, Consumer, Exchange, Queue
from kombu.consume_ledger import (
    KIND_CHANNEL_CANCEL,
    KIND_CHANNEL_CONSUME,
    KIND_CHANNEL_GET,
    KIND_CONSUME,
    KIND_CANCEL_WITHHELD,
    KIND_DETACH_REFUSED,
    KIND_HUB_SCHEDULE,
    KIND_LATCH,
    KIND_QUEUE_DETACH,
    KIND_REVIVE,
    KIND_SIMPLE_GET_NOWAIT,
    PLANE_CONSUMER,
    PLANE_SIMPLE,
    PLANE_SQS,
    PLANE_VIRTUAL,
    PLANE_REDIS,
    STATUS_COMMITTED,
    STATUS_DEFERRED,
    STATUS_WITHHELD,
    get_ledger,
    ledger_for,
)
from kombu.exceptions import ConsumeLedgerProtected
from kombu.simple import SimpleQueue

# -- shared fake transports from the existing transport test modules ------

from t.unit.transport.test_redis import Transport as RedisTransport

boto3 = pytest.importorskip('boto3')

from kombu.asynchronous import set_event_loop  # noqa: E402
from kombu.transport import SQS  # noqa: E402
from t.unit.transport.test_SQS import SQSClientMock  # noqa: E402

DEFAULT_EXCHANGE = Exchange('')


# -- helpers ---------------------------------------------------------------

def make_queue(conn, name, channel=None):
    queue = Queue(name, DEFAULT_EXCHANGE, name)
    queue.maybe_bind(conn)
    queue.declare(channel=channel)
    return queue


def open_memory():
    conn = Connection('memory://')
    return conn, conn.channel()


def open_redis():
    conn = Connection(transport=RedisTransport)
    channel = conn.default_channel
    return conn, channel


def redis_declare(channel, name):
    channel.exchange_declare(name)
    channel.queue_declare(name)
    channel.queue_bind(name, name, name)
    return name


@pytest.fixture
def sqs_factory():
    """Factory for mocked SQS channels (with optional hub)."""
    SQS.Channel._queue_cache.clear()
    saved_sqs = SQS.Channel.sqs
    saved_noack = set(SQS.Channel._noack_queues)
    client = SQSClientMock(QueueName='unittest_queue')
    SQS.Channel.sqs = lambda self, queue=None: client
    created = []

    def factory(with_hub=False):
        if with_hub:
            from kombu.asynchronous import Hub
            hub = Hub()
            set_event_loop(hub)
            conn = Connection(transport=SQS.Transport)
            channel = conn.channel()
            channel.hub = hub
        else:
            set_event_loop(None)
            conn = Connection(transport=SQS.Transport)
            channel = conn.channel()
            channel.hub = None
        created.append((conn, channel))
        return conn, channel

    yield factory

    SQS.Channel.sqs = saved_sqs
    SQS.Channel._noack_queues.clear()
    SQS.Channel._noack_queues.update(saved_noack)
    set_event_loop(None)
    for conn, _ in created:
        conn.close()


def sqs_declare(conn, channel, name):
    queue = Queue(name, Exchange('sqs-direct', type='direct'), name)
    queue.maybe_bind(conn)
    queue.declare(channel=channel)
    return queue


def cancel_chain(ledger, tag):
    """All cancellation records for one tag, in causal order."""
    return ledger.filter(tag=tag, kind=KIND_CHANNEL_CANCEL)


# --------------------------------------------------------------------------
# 1. Same-channel nested scopes must not bleed into each other
# --------------------------------------------------------------------------

class test_NestedScopeIsolation:

    def test_inner_consume_cancel_cancel_by_queue_keeps_outer_state(self):
        conn, channel = open_memory()
        outer_q = make_queue(conn, 'iso-outer', channel)
        inner_q = make_queue(conn, 'iso-inner', channel)
        detach_q = make_queue(conn, 'iso-detach', channel)

        outer = Consumer(channel, queues=[outer_q], auto_declare=False)
        with channel.consume_ledger() as ledger:
            outer.consume()
            outer_tag = outer._active_tags['iso-outer']
            outer_scope = ledger.top.id

            with ledger.scope() as inner_frame:
                # New consumer registration owned by the inner frame.
                inner = Consumer(channel, queues=[inner_q], auto_declare=False)
                inner.consume()
                inner_tag = inner._active_tags['iso-inner']
                inner_scope = inner_frame.id

                # Bulk cancel of the inner consumer commits the inner tag.
                inner.cancel()
                assert not ledger.is_live(inner_tag)

                # Bulk-cancelling the OUTER consumer from inside the frame
                # must withhold the outer-committed tag instead of tearing
                # it down through the inner scope.
                outer.cancel()
                withheld = ledger.filter(kind=KIND_CANCEL_WITHHELD,
                                         tag=outer_tag)
                assert len(withheld) == 1
                assert withheld[0].scope == inner_scope
                assert withheld[0].status == STATUS_WITHHELD
                assert ledger.is_live(outer_tag)
                assert outer_tag in outer._active_tags.values()

                # cancel_by_queue on an inner-owned queue detaches for good.
                detacher = Consumer(channel, queues=[detach_q],
                                    auto_declare=False)
                detacher.consume()
                detach_tag = detacher._active_tags['iso-detach']
                detacher.cancel_by_queue(detach_q)
                assert 'iso-detach' not in detacher._queues
                assert not ledger.is_live(detach_tag)

                # Explicit cross-frame destruction is refused, not silently
                # written through the outer registration.
                with pytest.raises(ConsumeLedgerProtected):
                    channel.basic_cancel(outer_tag)
                with pytest.raises(ConsumeLedgerProtected):
                    outer.cancel_by_queue('iso-outer')
                refused = ledger.filter(kind=KIND_DETACH_REFUSED)
                assert len(refused) == 1
                assert refused[0].scope == inner_scope

            # Inner frame closed: committed cancels/detaches stay committed
            # (no undo), the outer registration is still live and attached.
            assert ledger.is_live(outer_tag)
            assert not ledger.is_live(inner_tag)
            assert not ledger.is_live(detach_tag)
            assert 'iso-outer' in outer._queues
            assert 'iso-detach' not in detacher._queues
            assert ledger.depth == 1
            assert ledger.top.id == outer_scope

            # Records written in the inner frame keep their attribution;
            # closing the frame never erased or rewrote them.
            outer_cancels = cancel_chain(ledger, outer_tag)
            assert outer_cancels == ()
            assert ledger.cancel_record(outer_tag) is None
            inner_cancels = cancel_chain(ledger, inner_tag)
            assert len(inner_cancels) == 1
            assert inner_cancels[0].scope == inner_scope

    def test_inner_simple_get_and_get_nowait_keep_outer_latch_and_tag(self):
        conn, channel = open_memory()
        queue = make_queue(conn, 'iso-simple', channel)
        producer = conn.Producer()
        producer.publish('one', routing_key='iso-simple')

        simple = SimpleQueue(channel, queue)
        with channel.consume_ledger() as ledger:
            simple.get(timeout=1)  # sets the one-shot latch + a tag
            latch_records = ledger.filter(kind=KIND_LATCH)
            assert len(latch_records) == 1
            assert latch_records[0].detail['phase'] == 'set'
            stag = list(ledger.live_tags())[0]
            outer_scope = ledger.top.id

            with ledger.scope() as inner:
                # get_nowait never creates a consumer tag nor flips latch.
                with pytest.raises(Empty):
                    simple.get_nowait()
                nowait_records = ledger.filter(kind=KIND_SIMPLE_GET_NOWAIT)
                assert len(nowait_records) == 1
                assert nowait_records[0].tag is None
                assert nowait_records[0].scope == inner.id

                # close() bulk-cancels: the outer tag must be withheld and
                # stay live, the latch must stay committed.
                simple.close()
                assert ledger.is_live(stag)
                assert simple._consuming is True
                withheld = ledger.filter(kind=KIND_CANCEL_WITHHELD,
                                         tag=stag)
                assert len(withheld) == 1
                assert withheld[0].scope == inner.id

                # Another blocking get reuses the latch: no new tag.
                producer.publish('two', routing_key='iso-simple')
                simple.get(timeout=1)
                latch_after = ledger.filter(kind=KIND_LATCH)
                assert [r.detail['phase'] for r in latch_after] == [
                    'set', 'reused']
                assert ledger.live_tags() == {stag}

            assert ledger.is_live(stag)
            assert simple._consuming is True
            assert ledger.top.id == outer_scope

    def test_committed_steps_are_never_undone_by_scope_exit(self):
        conn, channel = open_memory()
        q1 = make_queue(conn, 'perm-1', channel)
        q2 = make_queue(conn, 'perm-2', channel)
        consumer = Consumer(channel, queues=[q1, q2], auto_declare=False)

        with channel.consume_ledger() as ledger:
            consumer.consume()
            tag1 = consumer._active_tags['perm-1']

            with ledger.scope():
                # A queue attached AND detached inside the inner frame: its
                # detach is a permanent commit, not an undoable intent.
                detacher = Consumer(channel, queues=[q2], auto_declare=False)
                detacher.consume()
                tag2 = detacher._active_tags['perm-2']
                detacher.cancel_by_queue('perm-2')
                assert tag2 not in detacher._active_tags.values()
                assert not ledger.is_live(tag2)
                assert ledger.detached_queues(id(detacher)) == {'perm-2'}

            # Exiting the frame must not re-attach the queue or re-live tag.
            assert 'perm-2' not in detacher._queues
            assert not ledger.is_live(tag2)
            assert ledger.detached_queues(id(detacher)) == {'perm-2'}
            final_cancel = ledger.cancel_record(tag2)
            assert final_cancel is not None
            assert final_cancel.status == STATUS_COMMITTED
            # The untouched outer registration is unaffected.
            assert ledger.is_live(tag1)

    def test_inner_claims_merge_to_parent_after_commit(self):
        """Once the inner frame closes cleanly, its surviving live tags
        become part of the parent frame's world: a later outer bulk cancel
        cancels them (they were committed, not rolled back)."""
        conn, channel = open_memory()
        outer_q = make_queue(conn, 'merge-outer', channel)
        inner_q = make_queue(conn, 'merge-inner', channel)
        consumer = Consumer(channel, queues=[outer_q], auto_declare=False)

        with channel.consume_ledger() as ledger:
            consumer.consume()
            outer_tag = consumer._active_tags['merge-outer']

            inner_consumer = None
            with ledger.scope():
                inner_consumer = Consumer(channel, queues=[inner_q],
                                          auto_declare=False)
                inner_consumer.consume()
                inner_tag = inner_consumer._active_tags['merge-inner']
                # Deliberately leave the inner registration live.
            assert ledger.is_live(inner_tag)

            # Outer bulk cancel now takes down both registrations.
            consumer.cancel()
            inner_consumer.cancel()
            assert not ledger.is_live(outer_tag)
            assert not ledger.is_live(inner_tag)

    def test_no_ledger_keeps_pin_behaviour(self):
        conn, channel = open_memory()
        assert ledger_for(channel) is None
        queue = make_queue(conn, 'pin-1', channel)
        consumer = Consumer(channel, queues=[queue], auto_declare=False)
        consumer.consume()
        tag = consumer._active_tags['pin-1']
        consumer.cancel()  # pin: clears every mapping
        assert consumer._active_tags == {}
        assert tag not in channel._consumers
        # Still no ledger got created by the pin exits.
        assert ledger_for(channel) is None


# --------------------------------------------------------------------------
# 2. Consumer plane: cancel vs cancel_by_queue, revive, nowait fork
# --------------------------------------------------------------------------

class test_ConsumerCommitDivergence:

    def test_cancel_keeps_queues_but_cancel_by_queue_detaches(self):
        conn, channel = open_memory()
        keep_q = make_queue(conn, 'div-keep', channel)
        drop_q = make_queue(conn, 'div-drop', channel)

        # cancel(): tags cancelled, queue membership retained.
        keeper = Consumer(channel, queues=[keep_q], auto_declare=False)
        with channel.consume_ledger() as ledger:
            keeper.consume()
            keep_tag = keeper._active_tags['div-keep']
            keeper.cancel()

            assert 'div-keep' in keeper._queues
            assert not ledger.is_live(keep_tag)
            cancel_record = ledger.cancel_record(keep_tag)
            assert cancel_record is not None
            assert cancel_record.kind == KIND_CHANNEL_CANCEL
            assert (ledger.filter(kind=KIND_QUEUE_DETACH,
                                  queue='div-keep') == ())

        # cancel_by_queue() on a second consumer: tag cancelled AND queue
        # popped -- a different commit kind on the same channel.
        with channel.consume_ledger() as ledger2:
            dropper = Consumer(channel, queues=[drop_q], auto_declare=False)
            dropper.consume()
            drop_tag = dropper._active_tags['div-drop']
            ledger2.register_owner(dropper)
            dropper.cancel_by_queue(drop_q)

            assert 'div-drop' not in dropper._queues
            assert not ledger2.is_live(drop_tag)
            detach = ledger2.last(kind=KIND_QUEUE_DETACH)
            assert detach.queue == 'div-drop'
            assert detach.detail['tag_cancelled'] is True
            assert detach.detail['register_seq'] < detach.seq
            cancel_record = ledger2.cancel_record(drop_tag)
            assert cancel_record.status == STATUS_COMMITTED
            assert cancel_record.detail['register_seq'] < cancel_record.seq

    def test_revive_invalidates_tags_without_auto_consume(self):
        conn, channel = open_memory()
        queue = make_queue(conn, 'rev-1', channel)
        consumer = Consumer(channel, queues=[queue], auto_declare=True)

        with channel.consume_ledger() as ledger:
            consumer.consume()
            dead_tag = consumer._active_tags['rev-1']
            register_seq = ledger.register_seq(dead_tag)
            assert register_seq is not None

            consumer.revive(channel)

            # Revive is an invalidation commit, not a cancellation commit.
            revive = ledger.last(kind=KIND_REVIVE)
            assert revive is not None
            invalidated = dict(revive.detail['invalidated'])
            assert invalidated == {'rev-1': dead_tag}
            assert revive.detail['auto_declare'] is True
            assert not ledger.is_live(dead_tag)
            assert ledger.cancel_record(dead_tag) is None
            assert consumer._active_tags == {}

            # Crucially nothing re-subscribed by itself: no consume record
            # exists after the revive commit.
            later = [r for r in ledger.records if r.seq > revive.seq]
            assert [r for r in later if r.kind == KIND_CONSUME] == []
            assert channel.basic_consume  # sanity: method intact

            # Explicit re-consume diverges from revive: a new tag is born.
            consumer.consume()
            new_tag = consumer._active_tags['rev-1']
            assert new_tag != dead_tag
            assert ledger.is_live(new_tag)
            new_register = [
                r for r in ledger.records
                if r.seq > revive.seq and r.kind == KIND_CONSUME
            ]
            assert len(new_register) == 1
            assert new_register[0].tag == new_tag
            assert new_register[0].seq > revive.seq

    def test_multi_queue_nowait_fork(self):
        conn, channel = open_memory()
        queues = [make_queue(conn, f'nw-{i}', channel) for i in range(3)]
        consumer = Consumer(channel, queues=queues, auto_declare=False)

        with channel.consume_ledger() as ledger:
            consumer.consume()

        registrations = ledger.filter(kind=KIND_CONSUME)
        assert [r.queue for r in registrations] == ['nw-0', 'nw-1', 'nw-2']
        # Prefix queues subscribe nowait, the terminal queue waits.
        assert [r.detail['nowait'] for r in registrations] == [
            True, True, False]
        # The transport got one basic_consume per queue, same causal order.
        channel_consumes = ledger.filter(kind=KIND_CHANNEL_CONSUME)
        assert len(channel_consumes) == 3
        assert [r.queue for r in channel_consumes] == [
            'nw-0', 'nw-1', 'nw-2']
        for consumer_record, channel_record in zip(
                registrations, channel_consumes):
            assert channel_record.tag == consumer_record.tag
            assert channel_record.seq > 0


# --------------------------------------------------------------------------
# 3. SimpleBase plane: get latch vs get_nowait basic_get
# --------------------------------------------------------------------------

class test_SimpleCommitDivergence:

    def test_get_commits_latch_and_tag_get_nowait_does_not(self):
        conn, channel = open_memory()
        queue = make_queue(conn, 'sim-1', channel)
        conn.Producer().publish('hello', routing_key='sim-1')

        simple = SimpleQueue(channel, queue)
        with channel.consume_ledger() as ledger:
            message = simple.get(timeout=1)
            assert message.payload == 'hello'

            latch = ledger.filter(kind=KIND_LATCH)
            assert len(latch) == 1 and latch[0].detail['phase'] == 'set'
            consumer_regs = ledger.filter(kind=KIND_CONSUME)
            assert len(consumer_regs) == 1
            assert consumer_regs[0].queue == 'sim-1'
            assert ledger.live_tags() == {consumer_regs[0].tag}

            # get_nowait on an empty queue polls via basic_get only.
            with pytest.raises(Empty):
                simple.get_nowait()
            nowait_records = ledger.filter(kind=KIND_SIMPLE_GET_NOWAIT)
            assert len(nowait_records) == 1
            gets = ledger.filter(kind=KIND_CHANNEL_GET)
            assert len(gets) == 1
            assert gets[0].detail['delivered'] is False
            assert gets[0].tag is None
            # Still exactly one registration: get_nowait built no tag.
            assert len(ledger.filter(kind=KIND_CONSUME)) == 1

    def test_latch_is_one_shot_and_close_never_resets_it(self):
        conn, channel = open_memory()
        queue = make_queue(conn, 'sim-2', channel)
        conn.Producer().publish('a', routing_key='sim-2')

        simple = SimpleQueue(channel, queue)
        with channel.consume_ledger() as ledger:
            simple.get(timeout=1)
            tag = next(iter(ledger.live_tags()))

            conn.Producer().publish('b', routing_key='sim-2')
            simple.get(timeout=1)  # latch reused: no second basic_consume.
            phases = [r.detail['phase']
                      for r in ledger.filter(kind=KIND_LATCH)]
            assert phases == ['set', 'reused']
            assert ledger.live_tags() == {tag}

            simple.close()
            assert simple._consuming is True
            close_record = ledger.last(kind='simple_close')
            assert close_record.detail == {
                'latch_was': True, 'latch_reset': False}


# --------------------------------------------------------------------------
# 4. Transport completion planes: virtual immediate / redis deferred / SQS
# --------------------------------------------------------------------------

class test_CompletionPlanes:

    def test_virtual_plane_removes_callback_cycle_and_queue_immediately(self):
        conn, channel = open_memory()
        queue = make_queue(conn, 'virt-1', channel)
        with channel.consume_ledger() as ledger:
            channel.basic_consume(
                queue.name, False, lambda message: None, 'virt-tag')
            register = ledger.last(kind=KIND_CHANNEL_CONSUME)
            assert register.plane == PLANE_VIRTUAL
            assert register.detail['callback_registered'] is True

            # Synchronous completion: cancelling commits immediately.
            channel.basic_cancel('virt-tag')
            cancels = cancel_chain(ledger, 'virt-tag')
            assert len(cancels) == 1
            assert cancels[0].status == STATUS_COMMITTED
            assert cancels[0].detail['callback_removed'] is True
            assert cancels[0].detail['active_queue_removed'] is True
            assert cancels[0].detail['cycle_reset'] is True
            assert cancels[0].detail['register_seq'] == register.seq
            assert not ledger.is_live('virt-tag')
            assert 'virt-1' not in channel.connection._callbacks
            assert 'virt-tag' not in channel._consumers
            assert 'virt-1' not in channel._active_queues

    def test_redis_deferred_cancel_diverges_from_virtual_immediate(self):
        conn, channel = open_redis()
        name = redis_declare(channel, 'redis-def')
        cycle = channel.connection.cycle

        with channel.consume_ledger() as ledger:
            channel.basic_consume(name, True, lambda m: None, 'redis-tag')
            register = ledger.last(kind=KIND_CHANNEL_CONSUME)
            assert register.plane == PLANE_REDIS
            assert register.detail['cycle_update'] is True

            # Protected read: the cancel is only scheduled.
            cycle._in_protected_read = True
            try:
                channel.basic_cancel('redis-tag')
                deferred = ledger.cancel_record('redis-tag')
                assert deferred.status == STATUS_DEFERRED
                assert deferred.plane == PLANE_REDIS
                assert deferred.detail['after_read'] is True
                assert deferred.detail['register_seq'] == register.seq
                assert ledger.is_pending('redis-tag')
                # The structural teardown literally has not happened yet.
                assert 'redis-tag' in channel._consumers
                assert name in channel._tag_to_queue.values()
                assert len(cycle.after_read) == 1
            finally:
                cycle._in_protected_read = False

            # Read protection ends -> after_read runs the real cancel.
            callback = cycle.after_read.pop()
            callback()
            committed = ledger.cancel_record('redis-tag')
            assert committed.status == STATUS_COMMITTED
            assert committed.detail['deferred_seq'] == deferred.seq
            assert committed.detail['cycle_updated'] is True
            assert 'redis-tag' not in channel._consumers
            assert not ledger.is_pending('redis-tag')
            assert not ledger.is_live('redis-tag')

            # Causal chain: register -> deferred -> committed, same tag.
            history = ledger.history('redis-tag')
            kinds = [r.kind for r in history]
            assert kinds == [
                KIND_CHANNEL_CONSUME,
                KIND_CHANNEL_CANCEL,
                KIND_CHANNEL_CANCEL,
            ]
            statuses = [r.status for r in history[1:]]
            assert statuses == [STATUS_DEFERRED, STATUS_COMMITTED]
            seqs = [r.seq for r in history]
            assert seqs == sorted(seqs)

    def test_redis_immediate_cancel_outside_protected_read(self):
        conn, channel = open_redis()
        name = redis_declare(channel, 'redis-imm')
        with channel.consume_ledger() as ledger:
            channel.basic_consume(name, True, lambda m: None, 'redis-tag2')
            channel.basic_cancel('redis-tag2')
            cancels = cancel_chain(ledger, 'redis-tag2')
            assert len(cancels) == 1
            assert cancels[0].status == STATUS_COMMITTED
            assert 'deferred_seq' in cancels[0].detail
            assert cancels[0].detail['deferred_seq'] is None

    def test_deferred_cancel_survives_inner_scope_and_is_trusted_later(self):
        conn, channel = open_redis()
        name = redis_declare(channel, 'redis-survive')
        cycle = channel.connection.cycle

        with channel.consume_ledger() as ledger:
            channel.basic_consume(
                name, True, lambda m: None, 'redis-tag3')
            cycle._in_protected_read = True
            channel.basic_cancel('redis-tag3')
            deferred_seq = ledger.cancel_record('redis-tag3').seq
            assert ledger.is_pending('redis-tag3')

            # An inner frame comes and goes: it may not flush or drop the
            # outer pending cancel, nor cancel the pending tag itself.
            with ledger.scope():
                with pytest.raises(ConsumeLedgerProtected):
                    channel.basic_cancel('redis-tag3')
                assert ledger.is_pending('redis-tag3')
            assert len(cycle.after_read) == 1
            assert ledger.is_pending('redis-tag3')

            # The pending cancel even commits while a fresh inner frame is
            # open: it is trusted execution of an earlier commit.
            with ledger.scope():
                cycle._in_protected_read = False
                cycle.after_read.pop()()
                assert not ledger.is_live('redis-tag3')
                committed = ledger.cancel_record('redis-tag3')
                assert committed.status == STATUS_COMMITTED
                assert committed.detail['deferred_seq'] == deferred_seq

    def test_sqs_consume_schedules_hub_and_cancel_discards_noack_sync(
            self, sqs_factory):
        conn, channel = sqs_factory(with_hub=True)
        queue = sqs_declare(conn, channel, 'sqs-hub')

        with channel.consume_ledger() as ledger:
            channel.basic_consume(
                queue.name, True, lambda m: None, 'sqs-tag')

            # Hub plane: one asynchronous long-poll kick scheduled.
            schedule = ledger.filter(kind=KIND_HUB_SCHEDULE)
            assert len(schedule) == 1
            assert schedule[0].queue == queue.name
            assert schedule[0].detail == {
                'callback': '_schedule_queue', 'via': 'call_soon'}
            consume_record = ledger.last(kind=KIND_CHANNEL_CONSUME)
            assert consume_record.plane == PLANE_SQS
            assert consume_record.detail['hub_scheduled'] is True
            assert consume_record.detail['noack_add'] is True
            assert queue.name in channel._noack_queues

            # Cancel plane: synchronous discard, never an after_read defer.
            channel.basic_cancel('sqs-tag')
            assert queue.name not in channel._noack_queues
            cancels = cancel_chain(ledger, 'sqs-tag')
            assert len(cancels) == 1
            assert cancels[0].status == STATUS_COMMITTED
            assert cancels[0].plane == PLANE_SQS
            assert cancels[0].detail['noack_discarded'] is True
            assert cancels[0].detail['synchronous'] is True
            assert cancels[0].detail['callback_removed'] is True

    def test_sqs_without_hub_consume_does_not_schedule(self, sqs_factory):
        conn, channel = sqs_factory(with_hub=False)
        queue = sqs_declare(conn, channel, 'sqs-nohub')
        with channel.consume_ledger() as ledger:
            channel.basic_consume(
                queue.name, True, lambda m: None, 'sqs-tag2')
            assert ledger.filter(kind=KIND_HUB_SCHEDULE) == ()
            consume_record = ledger.last(kind=KIND_CHANNEL_CONSUME)
            assert consume_record.detail['hub_scheduled'] is False


# --------------------------------------------------------------------------
# 5. One semantics across Consumer + SimpleBase + every transport plane
# --------------------------------------------------------------------------

@pytest.mark.parametrize('transport', ['virtual', 'redis', 'sqs'])
def test_same_commit_semantics_on_one_channel(transport, sqs_factory):
    """Registration -> committed cancellation with a causal link must be
    observable through Consumer, SimpleBase and the transport completion
    plane of virtual, redis and SQS on the same channel."""
    if transport == 'virtual':
        conn, channel = open_memory()
        queue = make_queue(conn, f'common-{transport}', channel)
        plane = PLANE_VIRTUAL
    elif transport == 'redis':
        conn, channel = open_redis()
        name = redis_declare(channel, f'common-{transport}')
        queue = Queue(name, Exchange(''), name)
        queue.maybe_bind(conn)
        plane = PLANE_REDIS
    else:
        conn, channel = sqs_factory(with_hub=False)
        queue = sqs_declare(conn, channel, 'common-sqs')
        plane = PLANE_SQS

    with channel.consume_ledger() as ledger:
        # Consumer plane commits a registration and a cancellation.
        consumer = Consumer(channel, queues=[queue], auto_declare=False)
        consumer.consume()
        tag = consumer._active_tags[queue.name]

        consumer_reg = ledger.last(kind=KIND_CONSUME, tag=tag)
        channel_reg = ledger.last(kind=KIND_CHANNEL_CONSUME, tag=tag)
        assert consumer_reg is not None and channel_reg is not None
        assert consumer_reg.owner == id(consumer)
        assert channel_reg.plane == plane
        assert ledger.is_live(tag)

        consumer.cancel()
        cancel_record = ledger.cancel_record(tag)
        assert cancel_record is not None
        assert cancel_record.status == STATUS_COMMITTED
        assert cancel_record.plane == plane
        # The cancel commit carries a causal link back to one of the two
        # registration records and happens strictly after both of them.
        assert cancel_record.detail['register_seq'] in (
            channel_reg.seq, consumer_reg.seq)
        assert cancel_record.seq > consumer_reg.seq
        assert cancel_record.seq > channel_reg.seq
        assert not ledger.is_live(tag)

        # SimpleBase plane on the SAME channel: get_nowait polls without a
        # tag; close commits a simple_close record.
        simple = SimpleQueue(channel, queue)
        with pytest.raises(Empty):
            simple.get_nowait()
        nowait_record = ledger.last(kind=KIND_SIMPLE_GET_NOWAIT)
        assert nowait_record is not None
        assert nowait_record.tag is None
        assert nowait_record.owner == id(simple)
        channel_get = ledger.last(kind=KIND_CHANNEL_GET)
        assert channel_get is not None and channel_get.tag is None
        simple.close()
        close_record = ledger.last(kind='simple_close')
        assert close_record.owner == id(simple)

        # The ledger covers all three planes on one channel.
        observed_planes = {r.plane for r in ledger.records}
        assert PLANE_CONSUMER in observed_planes
        assert PLANE_SIMPLE in observed_planes
        assert plane in observed_planes


# --------------------------------------------------------------------------
# 6. Ledger object/scopes contract
# --------------------------------------------------------------------------

class test_LedgerContract:

    def test_scopes_are_nested_and_history_is_append_only(self):
        conn, channel = open_memory()
        q1 = make_queue(conn, 'contract-1', channel)
        q2 = make_queue(conn, 'contract-2', channel)
        consumer = Consumer(channel, queues=[q1, q2], auto_declare=False)

        with channel.consume_ledger() as ledger:
            root = ledger.top
            assert root.depth == 1 and root.protected_tags == frozenset()
            consumer.consume()
            with ledger.scope() as inner:
                assert inner.depth == 2
                assert inner.protects_tag(
                    consumer._active_tags['contract-1'])
                assert inner.protects_queue(id(consumer), 'contract-2')
                consumer.cancel()  # withholds both outer tags
            assert ledger.depth == 1
            assert inner.closed is True

            # Nothing in the inner frame rewrote root records.
            root_records = [r for r in ledger.records if r.scope == root.id]
            inner_records = [r for r in ledger.records if r.scope == inner.id]
            assert root_records and inner_records
            assert all(r.kind == KIND_CANCEL_WITHHELD
                       for r in inner_records)
            # Append only: seqs are a monotonic 1..n run.
            assert [r.seq for r in ledger.records] == list(
                range(1, len(ledger.records) + 1))
            # The outer tags survived the inner cancellation.
            assert set(consumer._active_tags) == {
                'contract-1', 'contract-2'}

    def test_get_ledger_is_idempotent_per_channel(self):
        conn, channel = open_memory()
        first = get_ledger(channel)
        second = get_ledger(channel)
        assert first is second
        with first.scope() as frame:
            assert second.top is frame

    def test_consumer_and_simple_open_scopes_against_same_ledger(self):
        conn, channel = open_memory()
        queue = make_queue(conn, 'owner-1', channel)
        consumer = Consumer(channel, queues=[queue], auto_declare=False)
        with consumer.consume_ledger() as ledger:
            assert ledger is get_ledger(channel)
            consumer.consume()
            simple = SimpleQueue(channel, queue)
            with simple.consume_ledger() as inner_ledger:
                assert inner_ledger is ledger
                assert ledger.depth == 2

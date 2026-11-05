"""Channel-bound consumption registration & cancellation commit ledger.

The ledger is a host-facing, append-only journal of the *commit steps* that
make up consuming on a single channel:

* consumer-tag registrations (``Consumer.consume`` -> ``basic_consume``),
* cancellation commits (``basic_cancel``) across every transport completion
  plane (virtual removes the callback/cycle immediately, redis may defer the
  removal into ``after_read`` while a protected read is in progress, SQS
  discards ``_noack_queues`` synchronously and schedules long polling through
  the hub),
* queue-membership detach (``Consumer.cancel_by_queue``), which is a separate
  commit from ``Consumer.cancel`` (which keeps the queues),
* consumer revival (``Consumer.revive``), which invalidates tags without
  re-subscribing,
* the one-shot ``SimpleBase`` consuming latch, fed by ``get`` (drain path,
  creates a tag) but never by ``get_nowait`` (``basic_get`` path).

A ledger is bound to exactly one channel.  It is inert unless a host opens a
scope with :func:`consume_ledger`; without a ledger every exit point keeps its
historical (pin) behaviour.  Scopes nest.  Records are immutable facts:
closing a scope never un-sends a ``basic_cancel``, never re-attaches a
detached queue, never flushes a deferred redis cancel and never rewrites the
outer scope's history -- it only closes the inner frame.  Explicit
cross-scope destruction of a registration committed by an outer scope raises
:class:`~kombu.exceptions.ConsumeLedgerProtected`; bulk cancellation
withholds such registrations (keeps them live) and journals the withholding.
"""

from __future__ import annotations

import weakref
from contextlib import contextmanager
from itertools import count

from .exceptions import ConsumeLedgerProtected

__all__ = (
    'ConsumeLedger', 'CommitRecord', 'LedgerScope',
    'consume_ledger', 'get_ledger', 'ledger_for', 'plane_of',
    'authorize_cancel',
    'PLANE_CONSUMER', 'PLANE_SIMPLE', 'PLANE_VIRTUAL', 'PLANE_REDIS',
    'PLANE_SQS',
    'KIND_CONSUME', 'KIND_CHANNEL_CONSUME', 'KIND_CHANNEL_CANCEL',
    'KIND_CANCEL_WITHHELD', 'KIND_QUEUE_DETACH', 'KIND_DETACH_REFUSED',
    'KIND_REVIVE', 'KIND_LATCH', 'KIND_SIMPLE_GET', 'KIND_SIMPLE_GET_NOWAIT',
    'KIND_SIMPLE_CLOSE', 'KIND_CHANNEL_GET', 'KIND_HUB_SCHEDULE',
    'STATUS_COMMITTED', 'STATUS_DEFERRED', 'STATUS_WITHHELD',
    'STATUS_REFUSED', 'STATUS_INVALIDATED',
)

# -- completion planes -----------------------------------------------------

PLANE_CONSUMER = 'consumer'
PLANE_SIMPLE = 'simple'
PLANE_VIRTUAL = 'virtual'
PLANE_REDIS = 'redis'
PLANE_SQS = 'sqs'

# -- record kinds ----------------------------------------------------------

#: Consumer plane: a ``basic_consume`` was issued for ``tag`` on ``queue``.
KIND_CONSUME = 'consume'
#: Transport plane: the channel registered the consumer.
KIND_CHANNEL_CONSUME = 'channel_consume'
#: Transport plane: the channel cancelled the consumer.  ``status`` tells
#: whether the structural teardown committed synchronously or was deferred
#: (redis ``after_read``).
KIND_CHANNEL_CANCEL = 'channel_cancel'
#: Consumer plane: bulk ``cancel`` withheld an outer-scope tag instead of
#: issuing ``basic_cancel`` for it.
KIND_CANCEL_WITHHELD = 'cancel_withheld'
#: Consumer plane: ``cancel_by_queue`` popped the queue from ``_queues``.
KIND_QUEUE_DETACH = 'queue_detach'
#: Consumer plane: an inner-scope detach of an outer-scope queue was refused.
KIND_DETACH_REFUSED = 'detach_refused'
#: Consumer plane: ``revive`` invalidated live tags without cancelling them.
KIND_REVIVE = 'revive'
#: Simple plane: the one-shot ``_consuming`` latch transition (set/reused).
KIND_LATCH = 'simple_latch'
#: Simple plane: ``get`` took the drain_events path.
KIND_SIMPLE_GET = 'simple_get'
#: Simple plane: ``get_nowait`` took the basic_get path (no consumer tag).
KIND_SIMPLE_GET_NOWAIT = 'simple_get_nowait'
#: Simple plane: ``close`` delegated to ``consumer.cancel``.
KIND_SIMPLE_CLOSE = 'simple_close'
#: Transport plane: a synchronous ``basic_get`` happened (no tag involved).
KIND_CHANNEL_GET = 'channel_get'
#: SQS plane: ``_loop1`` scheduled ``_schedule_queue`` on the hub.
KIND_HUB_SCHEDULE = 'hub_schedule'

# -- record statuses -------------------------------------------------------

STATUS_COMMITTED = 'committed'
STATUS_DEFERRED = 'deferred'
STATUS_WITHHELD = 'withheld'
STATUS_REFUSED = 'refused'
STATUS_INVALIDATED = 'invalidated'

_CLAIM_LIVE = 'live'
_CLAIM_PENDING = 'cancel_pending'

_ATTR_LEDGER = '_kombu_consume_ledger'


class CommitRecord:
    """One immutable consumption-registration or cancellation commit fact.

    Attributes:
    ----------
        seq (int): Monotonic, channel-local sequence number.  Causal order
            on the same channel is the ordering of ``seq``.
        scope (int | None): Id of the ledger frame the commit happened in
            (``None`` for baseline commits made outside any open frame).
        depth (int): Frame nesting depth (``0`` outside every frame).
        plane (str): ``consumer``, ``simple``, ``virtual``, ``redis`` or
            ``sqs``.
        kind (str): One of the ``KIND_*`` constants.
        owner (int): ``id()`` of the owning Consumer / SimpleBase / channel.
        queue (str | None): Queue name the fact is about.
        tag (str | None): Consumer tag the fact is about.
        status (str): One of the ``STATUS_*`` constants.
        detail (dict): Plane specific structural facts (``nowait``,
            ``callback_removed``, ``deferred_seq``, ``hub_scheduled`` ...).
    """

    __slots__ = (
        'seq', 'scope', 'depth', 'plane', 'kind', 'owner', 'queue', 'tag',
        'status', 'detail',
    )

    def __init__(self, seq, scope, depth, plane, kind, owner,
                 queue=None, tag=None, status=STATUS_COMMITTED, detail=None):
        self.seq = seq
        self.scope = scope
        self.depth = depth
        self.plane = plane
        self.kind = kind
        self.owner = owner
        self.queue = queue
        self.tag = tag
        self.status = status
        self.detail = dict(detail or ())

    def __getitem__(self, key):
        return self.detail[key]

    def get(self, key, default=None):
        return self.detail.get(key, default)

    def __contains__(self, key):
        return key in self.detail

    def __repr__(self):
        return (
            f'<CommitRecord #{self.seq} scope={self.scope} '
            f'{self.plane}.{self.kind} status={self.status} '
            f'queue={self.queue!r} tag={self.tag!r} detail={self.detail!r}>'
        )


class _Claim:
    __slots__ = ('queue', 'owner', 'scope', 'state')

    def __init__(self, queue, owner, scope, state):
        self.queue = queue
        self.owner = owner
        self.scope = scope
        self.state = state


class LedgerScope:
    """A single open (or closed) ledger registration frame.

    The frame snapshots, at entry, everything that was already committed on
    the channel: live consumer tags, per-owner queue membership and latched
    SimpleBase instances.  That snapshot is the boundary that prevents an
    inner frame from writing through outer commits.
    """

    __slots__ = (
        'id', 'depth', 'protected_tags', 'protected_queues',
        'protected_latches', 'closed', 'record_seqs',
    )

    def __init__(self, scope_id, depth, protected_tags,
                 protected_queues, protected_latches):
        self.id = scope_id
        self.depth = depth
        self.protected_tags = protected_tags
        self.protected_queues = protected_queues
        self.protected_latches = protected_latches
        self.closed = False
        self.record_seqs = []

    def protects_tag(self, tag):
        return tag is not None and tag in self.protected_tags

    def protects_queue(self, owner, qname):
        return qname in self.protected_queues.get(owner, ())

    def protects_latch(self, owner):
        return owner in self.protected_latches

    def __repr__(self):
        state = 'closed' if self.closed else 'open'
        return (f'<LedgerScope id={self.id} depth={self.depth} {state} '
                f'tags={sorted(self.protected_tags)!r}>')


class ConsumeLedger:
    """Append-only consumption registration & cancellation commit ledger.

    One ledger per channel.  Obtain it through :func:`get_ledger` (or
    :func:`consume_ledger` for a scope) and inspect :attr:`records` for the
    ordered causal history.
    """

    def __init__(self, channel):
        self.channel = channel
        self._records = []
        self._frames = []
        self._claims = {}
        self._deferred = {}
        self._owners = {}
        self._trusted = set()
        self._seq = count(1)
        self._scope_ids = count(1)

    # -- scopes -----------------------------------------------------------

    @contextmanager
    def scope(self, owner=None):
        """Open a nested registration / commit scope.

        Arguments:
        ---------
            owner: Optional Consumer/SimpleBase that is registered with the
                ledger before the frame opens, so its queue membership and
                latch state become part of the protected baseline.
        """
        if owner is not None:
            self.register_owner(owner)
        frame = self._push()
        try:
            yield frame
        finally:
            frame.closed = True
            self._frames.pop()

    def register_owner(self, owner):
        """Register a Consumer/SimpleBase living on this channel."""
        self._owners[id(owner)] = weakref.ref(owner)

    def _alive_owners(self):
        dead = []
        for owner_id, ref in self._owners.items():
            owner = ref()
            if owner is None:
                dead.append(owner_id)
            else:
                yield owner_id, owner
        for owner_id in dead:
            self._owners.pop(owner_id, None)

    def _push(self):
        # Everything claimed before the frame opened is protected: live
        # tags as well as cancels already scheduled but not yet run
        # (redis after_read deferral).
        protected_tags = frozenset(self._claims)
        protected_queues = {}
        protected_latches = set()
        for owner_id, owner in self._alive_owners():
            queues = owner._ledger_queue_names()
            if queues:
                protected_queues[owner_id] = frozenset(queues)
            if owner._ledger_latch():
                protected_latches.add(owner_id)
        frame = LedgerScope(
            scope_id=next(self._scope_ids),
            depth=len(self._frames) + 1,
            protected_tags=protected_tags,
            protected_queues=protected_queues,
            protected_latches=frozenset(protected_latches),
        )
        self._frames.append(frame)
        return frame

    @property
    def top(self):
        """The innermost open frame, or ``None``."""
        return self._frames[-1] if self._frames else None

    @property
    def depth(self):
        return len(self._frames)

    # -- recording --------------------------------------------------------

    def record(self, *, plane, kind, owner, queue=None, tag=None,
               status=STATUS_COMMITTED, **detail):
        """Append an immutable commit fact and return it."""
        frame = self.top
        record = CommitRecord(
            seq=next(self._seq),
            scope=frame.id if frame is not None else None,
            depth=frame.depth if frame is not None else 0,
            plane=plane, kind=kind, owner=owner,
            queue=queue, tag=tag, status=status, detail=detail,
        )
        self._records.append(record)
        if frame is not None:
            frame.record_seqs.append(record.seq)
        return record

    @property
    def records(self):
        """Tuple of all commit facts, in causal (seq) order."""
        return tuple(self._records)

    def filter(self, *, plane=None, kind=None, status=None,
               queue=None, tag=None, owner=None, scope=None):
        """Return records matching every given facet."""
        out = []
        for record in self._records:
            if plane is not None and record.plane != plane:
                continue
            if kind is not None and record.kind != kind:
                continue
            if status is not None and record.status != status:
                continue
            if queue is not None and record.queue != queue:
                continue
            if tag is not None and record.tag != tag:
                continue
            if owner is not None and record.owner != owner:
                continue
            if scope is not None and record.scope != scope:
                continue
            out.append(record)
        return tuple(out)

    def last(self, **filters):
        """Latest matching record, or ``None``."""
        matches = self.filter(**filters)
        return matches[-1] if matches else None

    def kinds(self):
        """All record kinds in causal order (handy for causal assertions)."""
        return [record.kind for record in self._records]

    def history(self, tag):
        """Every record mentioning ``tag``, in causal order."""
        return tuple(r for r in self._records if r.tag == tag)

    # -- tag claim table --------------------------------------------------

    def claim_tag(self, tag, queue, owner):
        """Mark ``tag`` as a live registration claimed by the current frame.

        The first claim wins: Consumer-level claims are registered before
        the transport ``basic_consume`` runs, so a transport-plane
        registration never overwrites the owning Consumer attribution.
        """
        if tag in self._claims:
            return
        frame = self.top
        self._claims[tag] = _Claim(
            queue=queue, owner=owner,
            scope=frame.id if frame is not None else None,
            state=_CLAIM_LIVE,
        )

    def live_tags(self):
        """Set of tags with a live (non cancelled, non pending) claim."""
        return {
            tag for tag, claim in self._claims.items()
            if claim.state == _CLAIM_LIVE
        }

    def pending_tags(self):
        """Set of tags whose cancellation has been scheduled but not run."""
        return {
            tag for tag, claim in self._claims.items()
            if claim.state == _CLAIM_PENDING
        }

    def is_live(self, tag):
        claim = self._claims.get(tag)
        return claim is not None and claim.state == _CLAIM_LIVE

    def is_pending(self, tag):
        claim = self._claims.get(tag)
        return claim is not None and claim.state == _CLAIM_PENDING

    def queue_of(self, tag):
        claim = self._claims.get(tag)
        if claim is not None:
            return claim.queue
        record = self.last(tag=tag)
        return record.queue if record is not None else None

    def mark_cancel_pending(self, tag, record=None):
        """Move a live claim into the deferred-cancel-pending state."""
        claim = self._claims.get(tag)
        if claim is not None:
            claim.state = _CLAIM_PENDING
        if record is not None and tag is not None:
            self._deferred[tag] = record.seq

    def mark_cancelled(self, tag, record=None):
        """Drop the claim for ``tag`` (its ``basic_cancel`` committed)."""
        self._claims.pop(tag, None)
        if tag is not None:
            self._deferred.pop(tag, None)

    def invalidate_claims(self, tags):
        """Drop claims without a cancellation commit (e.g. connection loss).

        Returns the ``(queue, tag)`` pairs that were invalidated.
        """
        invalidated = []
        for tag in tags:
            claim = self._claims.pop(tag, None)
            self._deferred.pop(tag, None)
            if claim is not None:
                invalidated.append((claim.queue, tag))
        return invalidated

    @contextmanager
    def trusted(self, tag):
        """Mark ``tag`` as allowed to cancel even while an inner frame is
        open (used to run a previously scheduled, committed redis
        ``after_read`` cancel)."""
        self._trusted.add(tag)
        try:
            yield
        finally:
            self._trusted.discard(tag)

    # -- causal lookups ---------------------------------------------------

    def register_seq(self, tag):
        """Seq of the registration commit for ``tag``, if the ledger saw it."""
        record = self.last(tag=tag, kind=KIND_CONSUME)
        if record is None:
            record = self.last(tag=tag, kind=KIND_CHANNEL_CONSUME)
        return record.seq if record is not None else None

    def cancel_record(self, tag):
        """Latest cancellation record for ``tag`` (any status)."""
        records = [
            r for r in self._records
            if r.tag == tag and r.kind == KIND_CHANNEL_CANCEL
        ]
        return records[-1] if records else None

    def deferred_record(self, tag):
        seq = self._deferred.get(tag)
        if seq is None:
            return None
        return self._records[seq - 1] if 0 < seq <= len(self._records) else None

    def detached_queues(self, owner=None):
        return {
            r.queue for r in self._records
            if r.kind == KIND_QUEUE_DETACH
            and (owner is None or r.owner == owner)
            and r.queue is not None
        }

    def is_detached(self, owner, qname):
        return any(
            r.kind == KIND_QUEUE_DETACH and r.owner == owner
            and r.queue == qname
            for r in self._records
        )


# -- channel binding -------------------------------------------------------

_ledgers = weakref.WeakKeyDictionary()


def ledger_for(channel):
    """Return the ledger bound to ``channel``, or ``None`` when disabled."""
    if channel is None:
        return None
    try:
        ledger = _ledgers.get(channel)
    except TypeError:
        ledger = None
    if ledger is None:
        ledger = getattr(channel, _ATTR_LEDGER, None)
    return ledger


def get_ledger(channel):
    """Return (creating if needed) the ledger bound to ``channel``."""
    ledger = ledger_for(channel)
    if ledger is None:
        ledger = ConsumeLedger(channel)
        try:
            _ledgers[channel] = ledger
        except TypeError:
            # Non weak-referenceable channel objects: attach directly.
            try:
                setattr(channel, _ATTR_LEDGER, ledger)
            except Exception:  # pragma: no cover
                pass
    return ledger


@contextmanager
def consume_ledger(channel, owner=None):
    """Open a registration & cancellation commit scope on ``channel``."""
    with get_ledger(channel).scope(owner=owner):
        yield get_ledger(channel)


def plane_of(channel):
    """Name the transport completion plane of a channel object."""
    module = type(channel).__module__ or ''
    if module.endswith('redis'):
        return PLANE_REDIS
    if module.endswith('SQS'):
        return PLANE_SQS
    return PLANE_VIRTUAL


def authorize_cancel(channel, tag):
    """Refuse an explicit cancel targeting an outer-scope registration.

    Called from the transport ``basic_cancel`` completion planes.  A cancel
    is allowed when no frame is open, when the tag was not committed before
    the innermost frame opened, or when it is the trusted execution of a
    previously scheduled cancel.
    """
    ledger = ledger_for(channel)
    if ledger is None:
        return
    frame = ledger.top
    if frame is None:
        return
    if frame.protects_tag(tag) and tag not in ledger._trusted:
        raise ConsumeLedgerProtected(
            f'consumer tag {tag!r} is committed by an outer consume-ledger '
            f'scope (frame {frame.id}); close the inner scope before '
            f'cancelling it'
        )

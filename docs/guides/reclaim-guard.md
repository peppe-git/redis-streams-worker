# Preventing double execution with a ReclaimGuard

The worker is at-least-once. A lease keeps other workers away from a message while its owner
heartbeats, but a lease can expire while the owner is still alive. This guide shows the pattern that
makes double execution harmless when the work lives in a relational database.

## The claim

Keep, next to the work, who is executing it and until when:

```sql
CREATE TABLE job (
    id            uuid PRIMARY KEY,
    status        text NOT NULL,           -- queued, running, done, failed
    claimed_by    text,                    -- consumer name
    claim_expires timestamptz
);
```

The handler takes the claim in one statement, renews it while it works, and releases it at the end:

```sql
-- take: succeeds only if nobody holds a valid claim
UPDATE job
   SET claimed_by = :consumer, claim_expires = now() + interval '2 minutes', status = 'running'
 WHERE id = :id
   AND status IN ('queued', 'running')
   AND (claimed_by IS NULL OR claimed_by = :consumer OR claim_expires < now());
```

```java
MessageHandler handler = (message, ctx) -> {
    UUID id = UUID.fromString(message.field("jobId"));
    if (!jobs.tryClaim(id, ctx.consumerName())) {
        if (jobs.isFinished(id)) {
            return Outcome.ACK;              // already done by someone: drop the duplicate
        }
        return Outcome.LEAVE_PENDING;        // someone alive holds it: try again later
    }
    try {
        process(id);
        jobs.complete(id);                   // status = done, claimed_by = null
    } catch (Exception e) {
        jobs.fail(id, e);                    // recorded: no need to redeliver
    }
    return Outcome.ACK;
};
```

Renew the claim together with the lease, from a listener:

```java
WorkerListener renewClaims = new WorkerListener() {
    @Override
    public void onLeaseRenewed(QueueMessage message, boolean held) {
        if (held) {
            jobs.extendClaim(UUID.fromString(message.field("jobId")), consumerName);
        }
    }
};
```

## The guard

Without a guard, a worker that finds the message pending and without lease would claim it
(`XCLAIM`, delivery + 1), call the handler, get `LEAVE_PENDING`, and do it again on the next scan, until
the message is dead-lettered while the first owner is still working. The guard stops this before the
claim, so the delivery count does not move:

```java
ReclaimGuard guard = candidate -> {
    String jobId = candidate.message().field("jobId");
    return jobId == null || !jobs.claimedByAnotherLiveOwner(UUID.fromString(jobId));
};
```

```sql
SELECT claimed_by IS NOT NULL AND claim_expires > now() FROM job WHERE id = :id;
```

Notes:

- The guard is **fail-open**: if it throws (database down) the reclaim proceeds, and the handler's own
  claim is the last line of defence.
- When the previous owner really died, its claim expires (`claim_expires`), the guard says yes, and
  the message is reclaimed once.
- Choose the claim expiry longer than the lease TTL, and renew both together.

## Also useful

- `MessageContext.leaseHeld()` turns `false` when another worker took the message: check it before an
  irreversible side effect (an e-mail, a payment) and stop if it is lost.
- A `WorkKeyExtractor` on `jobId` avoids running two messages for the same job at once on one instance
  (a retry enqueued while the first attempt is still finishing).

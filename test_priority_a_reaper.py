import asyncio

from hive.persistence.sqlite import SqliteHiveRepository


def test_orphan_requeue_compare_and_set_observed_claim(tmp_path):
    asyncio.run(_orphan_requeue_compare_and_set_observed_claim(tmp_path))


async def _orphan_requeue_compare_and_set_observed_claim(tmp_path):
    repo = SqliteHiveRepository(str(tmp_path / 'queue.db'))
    await repo.init()
    async with repo.connection() as tx:
        await tx.execute("INSERT INTO jobs (id, submitter_urn, kind, description, status, started_at, claimed_by, claimed_at, last_update_at, claim_lease_expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ('j', 'u', 'test', 'd', 'running', 1, 'a', 1, 1, 2))
        await tx.commit()
        candidates = await repo.select_orphan_claim_candidates(tx, now=10, hard_cutoff=0, default_cutoff=0)
        snapshot = candidates[0]
        for field, value in [('claimed_by', 'b'), ('claim_lease_expires_at', 20), ('last_update_at', 9), ('status', 'claimed')]:
            await tx.execute(f'UPDATE jobs SET {field}=? WHERE id=?', (value, 'j'))
            assert await repo.requeue_orphan_job(tx, snapshot) == 0
            await tx.rollback()
        assert await repo.requeue_orphan_job(tx, snapshot) == 1
        await tx.commit()
        async with tx.execute('SELECT status, claimed_by FROM jobs WHERE id=?', ('j',)) as cur:
            assert tuple(await cur.fetchone()) == ('queued', None)


def test_stale_cancellation_rechecks_claim_and_releases_dependencies(tmp_path):
    asyncio.run(_stale_cancellation(tmp_path))


async def _stale_cancellation(tmp_path):
    from hive.bus.app import _cancel_stale_jobs

    repo = SqliteHiveRepository(str(tmp_path / 'queue.db'))
    await repo.init()
    async with repo.connection() as tx:
        for jid, result, deps in [('parent', None, None), ('child', None, '["parent"]'), ('parked', '{"infrastructure_parked":true}', None), ('race', None, None)]:
            await tx.execute("INSERT INTO jobs (id, submitter_urn, kind, description, status, started_at, result, depends_on) VALUES (?, ?, ?, ?, 'queued', ?, ?, ?)", (jid, 'u', 'test', 'd', 1 if jid != 'child' else 100, result, deps))
        await tx.commit()
        candidates = await repo.stale_cancel_candidates(tx, cutoff=10)
        race = next(c for c in candidates if c[0] == 'race')
        await tx.execute("UPDATE jobs SET status='claimed', claimed_by='worker', claim_lease_expires_at=20 WHERE id='race'")
        assert await repo.cancel_stale_candidate(tx, race, ended_at=10) == 0
        await tx.commit()
        assert await _cancel_stale_jobs(tx, repo, cutoff=10, now=10) == 1
        async with tx.execute('SELECT id, status, depends_on FROM jobs ORDER BY id') as cur:
            rows = {r[0]: (r[1], r[2]) for r in await cur.fetchall()}
        assert rows == {'child': ('queued', None), 'parent': ('cancelled', None), 'parked': ('queued', None), 'race': ('claimed', None)}

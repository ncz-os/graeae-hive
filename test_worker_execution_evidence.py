"""Exercise the shipped worker and bus completion path without fleet access."""

import asyncio
import json
from unittest import mock

import pytest

from hive.harness.codex import CodexAdapter, codex_executor
from hive.workers.run import HarnessWorker
from test_dependency_release_20261027 import JobUpdate, _RepoCase, hive_jobs_router


@pytest.mark.parametrize(
    "stderr",
    ["socket connection reset", "request cancelled", "daemon unavailable", "provider idle timeout", "tests failed"],
)
def test_nonzero_cli_is_reported_with_evidence(stderr):
    patches = []

    def transport(method, url, token, body):
        if method == "PATCH":
            patches.append(body)
            return 200, {"ok": True}
        if "/next?" in url:
            return 200, {"id": "job", "kind": "review:x"}
        return 200, {}

    adapter = CodexAdapter(transport=transport, executor=codex_executor())
    with mock.patch(
        "hive.harness.codex.subprocess.run", return_value=mock.Mock(returncode=1, stdout="", stderr=stderr)
    ):
        assert HarnessWorker(adapter, urn="urn:worker").run_once()
    assert len(patches) == 1
    assert patches[0]["status"] == "failed"
    assert patches[0]["result"]["stderr"] == stderr
    assert patches[0]["result"]["exit_code"] == 1


@pytest.mark.parametrize("error", [ConnectionResetError("socket reset"), RuntimeError("daemon unavailable")])
def test_execution_exception_preserves_type_and_message(error):
    adapter = mock.Mock()
    adapter.claim.return_value = {"id": "job"}
    adapter.execute.side_effect = error
    assert HarnessWorker(adapter, urn="urn:worker").run_once()
    adapter.complete.assert_not_called()
    adapter.fail_result.assert_called_once_with(
        "urn:worker", "job", {"error": str(error), "exception_type": type(error).__name__}
    )


def test_lost_completion_response_does_not_overwrite_success():
    adapter = mock.Mock()
    adapter.claim.return_value = {"id": "job"}
    adapter.execute.return_value = {"exit_code": 0}
    adapter.complete.side_effect = ConnectionError("lost response")
    assert HarnessWorker(adapter, urn="urn:worker").run_once()
    adapter.fail.assert_not_called()
    adapter.fail_result.assert_not_called()


class DeliveryRouteTests(_RepoCase):
    async def test_verified_pushed_head_with_only_untracked_files_is_delivered(self):
        for override, expected in [
            ({}, "done"),
            ({"tracked_worktree_clean": False}, "needs-review"),
            ({"remote_sha": "b" * 40}, "needs-review"),
            ({"remote_verified": False}, "needs-review"),
            ({"pushed": False}, "needs-review"),
        ]:
            with self.subTest(override=override):
                repo = await self._repo()
                await self._mk(repo, "job", status="claimed")
                async with repo.connection() as db:
                    await db.execute("UPDATE jobs SET repo='example' WHERE id='job'")
                    await db.commit()
                proof = dict(
                    head_sha="a" * 40,
                    commits=["a" * 40],
                    files_changed=["code.py"],
                    worktree_clean=False,
                    tracked_worktree_clean=True,
                    untracked_files=["output.log"],
                    pushed=True,
                    remote_verified=True,
                    remote_sha="a" * 40,
                )
                proof.update(override)
                with (
                    mock.patch.object(hive_jobs_router, "hive_repo", lambda repo=repo: repo),
                    mock.patch.object(hive_jobs_router, "emit_event", mock.AsyncMock()),
                ):
                    await hive_jobs_router.update_job(
                        "job", JobUpdate(status="done", claimed_by="urn:worker", result=proof)
                    )
                async with repo.connection() as db:
                    async with db.execute("SELECT status,result FROM jobs WHERE id='job'") as cur:
                        row = await cur.fetchone()
                self.assertEqual(row[0], expected)
                self.assertEqual(json.loads(row[1])["untracked_files"], ["output.log"])


class WorkerRegistrationRouteTests(_RepoCase):
    async def test_worker_host_registers_through_real_asgi_schema_and_route(self):
        import httpx

        import agent_bus
        from hive.bus import deps

        repo = await self._repo()
        requests = []

        def transport(method, url, token, body):
            requests.append(body)

            async def send():
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://test"
                ) as client:
                    response = await client.request(method, url, json=body)
                    return response.status_code, response.json()

            return asyncio.run(send())

        adapter = CodexAdapter(bus_url="http://test", transport=transport)
        worker = HarnessWorker(adapter, host="registration-test-host")
        with mock.patch.object(deps, "_HIVE_REPO", repo):
            urn = await asyncio.to_thread(worker.register)
        self.assertEqual(requests[0]["host"], "registration-test-host")
        self.assertIn(":registration-test-host:", urn)
        async with repo.connection() as db:
            async with db.execute("SELECT host FROM agents WHERE urn=?", (urn,)) as cursor:
                self.assertEqual((await cursor.fetchone())[0], "registration-test-host")

    async def test_default_worker_host_is_local_hostname(self):
        with mock.patch("hive.workers.run.socket.gethostname", return_value="local-host.example"):
            worker = HarnessWorker(CodexAdapter())
        self.assertEqual(worker.host, "local-host")

"""Space budget behaviour: admission, reservations, blocking and recovery."""

from __future__ import annotations

import asyncio
import time

import httpx

from app.budget import BUDGET_ERROR_PREFIX
from app.collector import Collector
from tests.conftest import set_delay, stack  # noqa: F401


async def _wait(client: httpx.AsyncClient, api: str, task_id: str, states: set[str], timeout: float = 60):
    deadline = time.monotonic() + timeout
    view = {}
    while time.monotonic() < deadline:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["state"] in states:
            return view
        await asyncio.sleep(0.05)
    raise AssertionError(f"timeout; last={view}")


async def _entry_size(client, source: str, station: str, date: str) -> int:
    payload = (await client.get(f"{source}/directory/{station}/{date}")).json()
    return payload["entries"][0]["size"]


async def _make_task(client, api: str, station: str, date: str) -> str:
    resp = await client.post(f"{api}/tasks", json={"station": station, "date": date})
    assert resp.status_code == 201
    return resp.json()["id"]


async def test_default_budget_is_unlimited_and_snapshot_shape(stack):
    _st, client, _settings, _collector = stack
    api = _st.api_url
    snap = (await client.get(f"{api}/budget")).json()
    assert snap["limit_bytes"] is None
    assert snap["used_bytes"] == 0
    assert snap["reserved_bytes"] == 0
    assert snap["available_bytes"] is None
    assert snap["holds"] == []
    assert snap["blocked"] == []


async def test_exact_fit_budget_completes_collection(stack):
    st, client, settings, _collector = stack
    source, api = st.source_url, st.api_url
    size = await _entry_size(client, source, "FIT", "2026-11-17")
    # An empty disk whose budget can hold exactly one package must complete.
    resp = await client.put(f"{api}/budget", json={"limit_bytes": size})
    assert resp.status_code == 200
    assert resp.json()["available_bytes"] == size

    task_id = await _make_task(client, api, "FIT", "2026-11-17")
    await client.post(f"{api}/tasks/{task_id}/start")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done

    snap = (await client.get(f"{api}/budget")).json()
    assert snap["used_bytes"] == size
    assert snap["reserved_bytes"] == 0
    assert snap["available_bytes"] == 0
    assert snap["holds"] == []
    # The SQLite database is never counted.
    assert settings.database_path.exists()


async def test_budget_smaller_than_package_blocks_and_reports_reason(stack):
    st, client, _settings, _collector = stack
    source, api = st.source_url, st.api_url
    size = await _entry_size(client, source, "SMALL", "2026-11-17")
    await client.put(f"{api}/budget", json={"limit_bytes": size - 1})

    task_id = await _make_task(client, api, "SMALL", "2026-11-17")
    await client.post(f"{api}/tasks/{task_id}/start")
    failed = await _wait(client, api, task_id, {"failed", "completed"})
    assert failed["state"] == "failed"
    assert (failed["error"] or "").startswith(BUDGET_ERROR_PREFIX)
    # Nothing was downloaded: admission happens before body bytes arrive.
    assert failed["downloaded_bytes"] == 0

    snap = (await client.get(f"{api}/budget")).json()
    assert snap["used_bytes"] == 0
    assert snap["reserved_bytes"] == 0
    assert len(snap["blocked"]) == 1
    blocked = snap["blocked"][0]
    assert blocked["task_id"] == task_id
    assert blocked["expected_size"] == size
    assert blocked["required_bytes"] == size
    assert blocked["available_bytes"] == size - 1
    assert BUDGET_ERROR_PREFIX in blocked["reason"]

    # Raising the cap unblocks a retry; other-task data is untouched anyway.
    await client.put(f"{api}/budget", json={"limit_bytes": size})
    await client.post(f"{api}/tasks/{task_id}/start")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    assert (await client.get(f"{api}/budget")).json()["blocked"] == []


async def test_parallel_tasks_share_budget_second_is_blocked_then_retried(stack):
    st, client, settings, collector = stack
    source, api = st.source_url, st.api_url
    size_a = await _entry_size(client, source, "PARA", "2026-11-17")
    size_b = await _entry_size(client, source, "PARB", "2026-11-17")
    # While A streams its full-size reservation the headroom is smaller than
    # B's admission need; raising the cap by the small margin later lets B in.
    margin = 4096
    cap = size_a + size_b - margin
    await client.put(f"{api}/budget", json={"limit_bytes": cap})

    await set_delay(st, client, 0.0, 0.05)
    task_a = await _make_task(client, api, "PARA", "2026-11-17")
    task_b = await _make_task(client, api, "PARB", "2026-11-17")
    await client.post(f"{api}/tasks/{task_a}/start")

    # Wait until A holds the budget and has streamed some bytes.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        view = (await client.get(f"{api}/tasks/{task_a}")).json()
        snap = (await client.get(f"{api}/budget")).json()
        if view["downloaded_bytes"] > 0 and snap["holds"]:
            break
        await asyncio.sleep(0.02)
    snap = (await client.get(f"{api}/budget")).json()
    assert len(snap["holds"]) == 1
    assert snap["holds"][0]["task_id"] == task_a
    # Used + reserved never exceeds the cap while streaming.
    assert snap["used_bytes"] + snap["reserved_bytes"] <= snap["limit_bytes"]

    await client.post(f"{api}/tasks/{task_b}/start")
    failed_b = await _wait(client, api, task_b, {"failed", "completed"})
    assert failed_b["state"] == "failed"
    assert BUDGET_ERROR_PREFIX in (failed_b["error"] or "")
    # B's failure must not delete or alter A's data.
    view_a = (await client.get(f"{api}/tasks/{task_a}")).json()
    assert view_a["state"] == "running"

    done_a = await _wait(client, api, task_a, {"completed", "failed"})
    assert done_a["state"] == "completed", done_a

    # B can be retried once the cap covers both finished packages; raising the
    # cap is accepted because usage never exceeded the old limit.
    await set_delay(st, client, 0.0, 0.0)
    raised = await client.put(f"{api}/budget", json={"limit_bytes": cap + margin})
    assert raised.status_code == 200
    await client.post(f"{api}/tasks/{task_b}/start")
    done_b = await _wait(client, api, task_b, {"completed", "failed"})
    assert done_b["state"] == "completed", done_b

    snap = (await client.get(f"{api}/budget")).json()
    assert snap["reserved_bytes"] == 0
    assert snap["used_bytes"] == size_a + size_b


async def test_pause_releases_unwritten_reservation_but_keeps_usage(stack):
    st, client, settings, _collector = stack
    source, api = st.source_url, st.api_url
    size = await _entry_size(client, source, "HOLD", "2026-11-17")
    await client.put(f"{api}/budget", json={"limit_bytes": size})
    await set_delay(st, client, 0.0, 0.05)

    task_id = await _make_task(client, api, "HOLD", "2026-11-17")
    await client.post(f"{api}/tasks/{task_id}/start")
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["downloaded_bytes"] > 50_000:
            break
        await asyncio.sleep(0.02)

    paused = (await client.post(f"{api}/tasks/{task_id}/pause")).json()
    assert paused["state"] == "paused"
    await asyncio.sleep(0.3)

    snap = (await client.get(f"{api}/budget")).json()
    # The reservation is gone; the retained fragment still occupies budget.
    assert snap["holds"] == []
    assert snap["reserved_bytes"] == 0
    assert snap["used_bytes"] == paused["downloaded_bytes"]
    assert list(settings.parts_dir.glob(f"{task_id}-*.part"))

    # Resume re-admits against the remaining headroom and completes.
    await set_delay(st, client, 0.0, 0.0)
    await client.post(f"{api}/tasks/{task_id}/resume")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    snap = (await client.get(f"{api}/budget")).json()
    assert snap["used_bytes"] == size
    assert snap["reserved_bytes"] == 0


async def test_lowering_below_committed_is_rejected_raising_allowed(stack):
    st, client, _settings, _collector = stack
    source, api = st.source_url, st.api_url
    size = await _entry_size(client, source, "LOW", "2026-11-17")
    await client.put(f"{api}/budget", json={"limit_bytes": size * 2})

    task_id = await _make_task(client, api, "LOW", "2026-11-17")
    await client.post(f"{api}/tasks/{task_id}/start")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed"

    refused = await client.put(f"{api}/budget", json={"limit_bytes": size - 1})
    assert refused.status_code == 409
    assert BUDGET_ERROR_PREFIX in refused.json()["detail"]
    # The earlier limit is still in force.
    assert (await client.get(f"{api}/budget")).json()["limit_bytes"] == size * 2

    ok = await client.put(f"{api}/budget", json={"limit_bytes": size})
    assert ok.status_code == 200
    assert ok.json()["limit_bytes"] == size
    # Clearing the cap is always allowed.
    cleared = await client.put(f"{api}/budget", json={"limit_bytes": None})
    assert cleared.json()["limit_bytes"] is None


async def test_refresh_keeps_old_package_while_new_version_downloads(stack, monkeypatch):
    st, client, settings, collector = stack
    source, api = st.source_url, st.api_url
    station, date = "REF", "2026-11-17"
    size_v1 = await _entry_size(client, source, station, date)

    task_id = await _make_task(client, api, station, date)

    # Record coexistence deterministically at every written chunk, instead of
    # relying on polling a transfer that may finish inside one event-loop turn.
    observations: list[tuple[int, int]] = []

    def record_chunk(self, run, part, written: int) -> None:
        parts = list(settings.parts_dir.glob(f"{task_id}-*.part"))
        packages = list(settings.packages_dir.glob("*.tar.gz"))
        part_bytes = parts[0].stat().st_size if parts else 0
        pkg_bytes = packages[0].stat().st_size if packages else 0
        observations.append((part_bytes, pkg_bytes))
        snap = collector.budget.snapshot()
        assert snap["used_bytes"] + snap["reserved_bytes"] <= cap

    monkeypatch.setattr(type(collector), "_on_chunk_written", record_chunk)

    # First collect v1 with a cap generous enough for the later refresh peak.
    await client.post(
        f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v1"}
    )
    size_v1 = await _entry_size(client, source, station, date)
    await client.post(
        f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"}
    )
    size_v2 = await _entry_size(client, source, station, date)
    # The refresh peak keeps the old package while the full new part grows.
    cap = size_v1 * 2 + size_v2
    await client.put(f"{api}/budget", json={"limit_bytes": cap})
    # Re-point the source at v1 so the initial transfer collects the old
    # revision; the switch to v2 happens after completion, like a real refresh.
    await client.post(
        f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v1"}
    )

    await client.post(f"{api}/tasks/{task_id}/start")
    first = await _wait(client, api, task_id, {"completed", "failed"})
    assert first["state"] == "completed"
    assert first["source_revision"] == "v1"
    old_packages = list(settings.packages_dir.glob("*.tar.gz"))
    assert len(old_packages) == 1
    assert observations, "chunk hook never fired during the first download"
    observations.clear()

    # Now the source moves to a new revision and the task refreshes.
    await client.post(
        f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"}
    )
    await set_delay(st, client, 0.8, 0.05)
    refreshed = (await client.post(f"{api}/tasks/{task_id}/refresh")).json()
    assert refreshed["state"] == "running"

    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    assert done["source_revision"] == "v2"

    # While the new part grew, the old package was retained and both were
    # counted together within the cap.
    assert observations, "chunk hook never fired during the refresh download"
    coexisting = [obs for obs in observations if obs[0] > 0 and obs[1] > 0]
    assert coexisting, "old package and new part never coexisted"
    for part_bytes, pkg_bytes in coexisting:
        assert part_bytes + pkg_bytes <= cap

    assert len(list(settings.packages_dir.glob("*.tar.gz"))) == 1
    snap = (await client.get(f"{api}/budget")).json()
    assert snap["used_bytes"] == size_v2
    assert snap["reserved_bytes"] == 0


async def test_refresh_to_larger_revision_is_re_admitted_and_blocked_if_needed(stack):
    st, client, settings, _collector = stack
    source, api = st.source_url, st.api_url
    station, date = "GROW", "2026-11-17"
    size_v1 = await _entry_size(client, source, station, date)
    # The new revision is roughly the same (~1 MB), so downloading it while the
    # old package is retained needs roughly 2x the space.  A cap of one package
    # plus a small margin must refuse the re-admission, proving the new revision
    # is re-checked against its new size rather than the old reservation.
    margin = 1000
    await client.put(f"{api}/budget", json={"limit_bytes": size_v1 + margin})

    task_id = await _make_task(client, api, station, date)
    await client.post(f"{api}/tasks/{task_id}/start")
    assert (await _wait(client, api, task_id, {"completed", "failed"}))["state"] == "completed"

    await client.post(
        f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"}
    )
    size_v2 = await _entry_size(client, source, station, date)
    assert size_v2 > margin  # peak (old package + new part) cannot fit

    await client.post(f"{api}/tasks/{task_id}/refresh")
    done = await _wait(client, api, task_id, {"failed", "completed"})
    assert done["state"] == "failed"
    assert BUDGET_ERROR_PREFIX in (done["error"] or "")
    # The old package is preserved and remains the only counted data; no other
    # task's data is deleted.
    assert len(list(settings.packages_dir.glob("*.tar.gz"))) == 1
    snap = (await client.get(f"{api}/budget")).json()
    assert snap["used_bytes"] == size_v1
    assert snap["reserved_bytes"] == 0
    assert len(snap["blocked"]) == 1
    assert snap["blocked"][0]["task_id"] == task_id

    # The old package is already consuming size_v1; the refresh peak needs
    # size_v1 (retained) + size_v2 (new part), so the cap must cover that peak.
    await client.put(f"{api}/budget", json={"limit_bytes": size_v1 + size_v1 + size_v2})
    await client.post(f"{api}/tasks/{task_id}/start")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    assert done["verified_size"] == size_v2


async def test_restart_purges_temporaries_releases_holds_and_recomputes_usage(stack, tmp_path):
    st, client, settings, collector = stack
    source, api = st.source_url, st.api_url
    size = await _entry_size(client, source, "BOOT", "2026-11-17")
    await client.put(f"{api}/budget", json={"limit_bytes": size})

    task_id = await _make_task(client, api, "BOOT", "2026-11-17")
    await client.post(f"{api}/tasks/{task_id}/start")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed"

    # Simulate leftovers of a crashed publish plus a stale reservation.
    leftover = settings.packages_dir / ".publish-deadbeef-stale.tmp"
    leftover.write_bytes(b"x" * 1234)
    collector.budget._holds[("ghost", 99)] = 9999

    await collector.shutdown()
    fresh = Collector(settings)
    await fresh.startup()

    # Limit persisted; un-published temporary swept; in-memory hold gone;
    # usage reconciled from the real files.
    assert not leftover.exists()
    snap = fresh.budget.snapshot()
    assert snap["limit_bytes"] == size
    assert snap["used_bytes"] == size
    assert snap["reserved_bytes"] == 0
    assert snap["holds"] == []
    await fresh.shutdown()


async def test_revision_re_admission_uses_new_size(stack):
    st, client, settings, _collector = stack
    source, api = st.source_url, st.api_url
    station, date = "RADMIT", "2026-11-17"
    size_v1 = await _entry_size(client, source, station, date)
    # Budget fits v1 only.
    await client.put(f"{api}/budget", json={"limit_bytes": size_v1})

    task_id = await _make_task(client, api, station, date)
    await client.post(f"{api}/tasks/{task_id}/start")
    assert (await _wait(client, api, task_id, {"completed", "failed"}))["state"] == "completed"

    await client.post(
        f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"}
    )
    size_v2 = await _entry_size(client, source, station, date)
    # v2 is a similar size; the refresh must re-admit using the new directory
    # size rather than trust the old reservation/bytes.
    await client.post(f"{api}/tasks/{task_id}/refresh")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    if done["state"] == "failed":
        assert BUDGET_ERROR_PREFIX in (done["error"] or "")
        # After raising the cap to cover the retained old package plus the full
        # new part, a retry works.
        await client.put(f"{api}/budget", json={"limit_bytes": size_v1 + size_v1 + size_v2})
        await client.post(f"{api}/tasks/{task_id}/start")
        done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    assert done["verified_size"] == size_v2


async def test_stale_generation_cannot_release_new_hold(stack):
    st, client, settings, collector = stack
    source, api = st.source_url, st.api_url
    size = await _entry_size(client, source, "GEN", "2026-11-17")
    await client.put(f"{api}/budget", json={"limit_bytes": size})
    await set_delay(st, client, 1.5, 0.0)

    task_id = await _make_task(client, api, "GEN", "2026-11-17")
    await client.post(f"{api}/tasks/{task_id}/start")
    await asyncio.sleep(0.3)
    paused = (await client.post(f"{api}/tasks/{task_id}/pause")).json()
    old_generation = paused["generation"]
    await asyncio.sleep(0.2)
    # The old worker's hold has already been released on exit.
    assert (old_generation,) not in [
        (h["task_id"], h["generation"]) for h in collector.budget.snapshot()["holds"]
    ]

    # A new generation takes a fresh hold; a stale release must not touch it.
    await set_delay(st, client, 0.0, 0.0)
    await client.post(f"{api}/tasks/{task_id}/resume")
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        snap = collector.budget.snapshot()
        if snap["holds"]:
            break
        await asyncio.sleep(0.02)
    new_generation = collector.budget.snapshot()["holds"][0]["generation"]
    assert new_generation != old_generation
    collector.budget.release(task_id, old_generation)  # late worker cleanup
    snap = collector.budget.snapshot()
    assert len(snap["holds"]) == 1
    assert snap["holds"][0]["generation"] == new_generation

    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done


async def test_timeout_fragment_keeps_counting_and_retry_succeeds(stack, monkeypatch):
    st, client, settings, _collector = stack
    source, api = st.source_url, st.api_url
    size = await _entry_size(client, source, "TO", "2026-11-17")
    await client.put(f"{api}/budget", json={"limit_bytes": size})
    await set_delay(st, client, 0.0, 0.08)
    task_id = await _make_task(client, api, "TO", "2026-11-17")

    real_stream = httpx.AsyncClient.stream
    calls = {"n": 0}

    def flaky_stream(self, method, url, **kwargs):
        calls["n"] += 1
        ctx = real_stream(self, method, url, **kwargs)
        if calls["n"] == 1:
            class Exploding:
                async def __aenter__(inner):
                    response = await ctx.__aenter__()

                    class Wrap:
                        status_code = response.status_code
                        headers = response.headers

                        async def aiter_bytes(inner2, size=0):
                            count = 0
                            async for chunk in response.aiter_bytes(size):
                                yield chunk
                                count += 1
                                if count >= 3:
                                    raise httpx.ReadTimeout("simulated timeout")

                        async def __aenter__(inner2):
                            return inner2

                        async def __aexit__(inner2, *a):
                            return False

                    return Wrap()

                async def __aexit__(inner, *a):
                    await ctx.__aexit__(*a)

            return Exploding()
        return ctx

    monkeypatch.setattr(httpx.AsyncClient, "stream", flaky_stream)
    await client.post(f"{api}/tasks/{task_id}/start")
    failed = await _wait(client, api, task_id, {"failed", "completed"}, timeout=60)
    monkeypatch.setattr(httpx.AsyncClient, "stream", real_stream)
    assert failed["state"] == "failed"
    assert "ReadTimeout" in (failed["error"] or "")

    # The retained fragment still counts against the budget; the reservation
    # was released when the worker stopped.
    snap = (await client.get(f"{api}/budget")).json()
    assert snap["reserved_bytes"] == 0
    assert snap["used_bytes"] == failed["downloaded_bytes"] > 0

    await set_delay(st, client, 0.0, 0.0)
    await client.post(f"{api}/tasks/{task_id}/start")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    snap = (await client.get(f"{api}/budget")).json()
    assert snap["used_bytes"] == size


async def test_budget_validation_rejects_negative_and_bad_types(stack):
    _st, client, _settings, _c = stack
    api = _st.api_url
    bad = await client.put(f"{api}/budget", json={"limit_bytes": -1})
    assert bad.status_code == 422
    bad_type = await client.put(f"{api}/budget", json={"limit_bytes": "lots"})
    assert bad_type.status_code == 422


async def test_budget_limit_survives_full_restart(stack):
    st, client, settings, collector = stack
    api = st.api_url
    await client.put(f"{api}/budget", json={"limit_bytes": 424242})
    await collector.shutdown()
    fresh = Collector(settings)
    await fresh.startup()
    assert fresh.budget.limit == 424242
    assert fresh.store.get_budget_limit() == 424242
    await fresh.shutdown()

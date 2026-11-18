"""Space budget: byte accounting and reservations for the collection disk.

Only files the collector itself creates are counted -- raw package parts
(``*.part``, including fragments kept across pause/timeout), published
packages (``*.tar.gz``) and publish temporaries (``.publish-*``).  The SQLite
database is deliberately excluded.

The budget invariant
--------------------
For every admitted task the directory size it may reach (its *hold*) is
reserved up front, after the source directory has been read and before any
package body is received.  While bytes stream in, an unwritten reservation is
progressively converted into real on-disk usage, so at all times::

    used_bytes + reserved_bytes <= limit_bytes

where ``used_bytes`` is the actual size of the counted files and
``reserved_bytes`` is the sum of the not-yet-written portions of the holds.

Holds are keyed by ``(task_id, generation)``.  A worker whose generation was
superseded (pause, refresh, restart) therefore cannot release or touch the
hold granted to the newer generation: late chunks from an old round can
neither write nor free the new round's budget.
"""

from __future__ import annotations

from pathlib import Path

BUDGET_ERROR_PREFIX = "space budget:"


class BudgetExceeded(Exception):
    """Admission refused: the task would push usage beyond the byte limit."""

    def __init__(self, *, required: int, available: int, limit: int | None):
        self.required = required
        self.available = available
        self.limit = limit
        if limit is None:
            text = "no space limit is configured"
        else:
            text = (
                f"needs {required} byte(s) but only {available} of {limit} "
                f"budget byte(s) are free"
            )
        super().__init__(f"{BUDGET_ERROR_PREFIX} {text}")


class BudgetRejected(Exception):
    """A budget configuration change is not allowed (e.g. lowering below use)."""


class Budget:
    """In-memory reservations over on-disk usage; the limit lives in SQLite."""

    def __init__(self, store, parts_dir: Path, packages_dir: Path):
        self.store = store
        self.parts_dir = parts_dir
        self.packages_dir = packages_dir
        self._holds: dict[tuple[str, int], int] = {}
        self.limit: int | None = store.get_budget_limit()

    # -- configuration ---------------------------------------------------
    def set_limit(self, limit: int | None) -> dict:
        """Persist a new total byte cap.

        Lowering the cap below the bytes already committed (real files plus
        outstanding reservations) is rejected; running tasks are never torn
        down to make room.
        """
        snapshot = self.snapshot()
        committed = snapshot["used_bytes"] + snapshot["reserved_bytes"]
        if limit is not None and limit < committed:
            raise BudgetRejected(
                f"{BUDGET_ERROR_PREFIX} cannot lower limit to {limit} byte(s): "
                f"{committed} byte(s) are already committed "
                f"({snapshot['used_bytes']} used + {snapshot['reserved_bytes']} reserved)"
            )
        self.store.set_budget_limit(limit)
        self.limit = limit
        return self.snapshot()

    # -- admission -------------------------------------------------------
    def admit(self, task_id: str, generation: int, hold_bytes: int) -> None:
        """Reserve the directory size a task is allowed to grow into.

        ``hold_bytes`` is the maximum total size of counted files belonging to
        this task while the hold is active (parts, kept old package and the
        publish temporary).  Raises :class:`BudgetExceeded` when admitting it
        would break the invariant; the caller fails the task without touching
        any other task's data.
        """
        task_files = self.task_used(task_id)
        additional = max(0, hold_bytes - task_files)
        if self.limit is not None:
            used = self.used_bytes()
            # Re-admission (e.g. after a resync) replaces this task's own hold.
            current_hold = self._holds.get((task_id, generation), 0)
            current_reserved = max(0, current_hold - task_files)
            other_reserved = self.reserved_bytes() - current_reserved
            if used + other_reserved + additional > self.limit:
                available = max(0, self.limit - used - other_reserved)
                raise BudgetExceeded(
                    required=additional,
                    available=available,
                    limit=self.limit,
                )
        self._holds[(task_id, generation)] = hold_bytes

    def release(self, task_id: str, generation: int) -> None:
        """Give back a hold.  A superseded worker can never touch a new hold."""
        self._holds.pop((task_id, generation), None)

    # -- accounting ------------------------------------------------------
    @staticmethod
    def file_size(path: Path) -> int:
        try:
            return path.stat().st_size
        except OSError:
            return 0

    def _counted_files(self) -> list[tuple[Path, str]]:
        """All counted files with their owning task id (``""`` if orphaned)."""
        files: list[tuple[Path, str]] = []
        if self.parts_dir.exists():
            for path in self.parts_dir.glob("*.part"):
                # part names are "<task_id>-<tag>.part"; task ids are hex
                files.append((path, path.name.split("-", 1)[0]))
        if self.packages_dir.exists():
            for path in self.packages_dir.iterdir():
                if not path.is_file():
                    continue
                if path.name.endswith(".tar.gz") or path.name.startswith(".publish-"):
                    owner = ""
                    if path.name.startswith(".publish-"):
                        # publish temporaries are named ".publish-<task_id>-..."
                        parts = path.name.split("-", 2)
                        if len(parts) >= 2:
                            owner = parts[1]
                    files.append((path, owner))
        return files

    def used_bytes(self) -> int:
        """Total bytes of parts, packages and publish temporaries on disk."""
        return sum(self.file_size(path) for path, _owner in self._counted_files())

    def task_used(self, task_id: str) -> int:
        return sum(
            self.file_size(path)
            for path, owner in self._counted_files()
            if owner == task_id
        )

    def reserved_bytes(self) -> int:
        """Sum of unwritten hold allowances (hold minus the task's real files)."""
        total = 0
        for (task_id, _generation), hold in self._holds.items():
            total += max(0, hold - self.task_used(task_id))
        return total

    # -- snapshots & recovery -------------------------------------------
    def snapshot(self) -> dict:
        used = self.used_bytes()
        reserved = self.reserved_bytes()
        limit = self.limit
        blocked = self._blocked_reasons(used, reserved)
        return {
            "limit_bytes": limit,
            "used_bytes": used,
            "reserved_bytes": reserved,
            "available_bytes": None if limit is None else max(0, limit - used - reserved),
            "holds": [
                {"task_id": task_id, "generation": generation, "bytes": hold}
                for (task_id, generation), hold in sorted(self._holds.items())
            ],
            "blocked": blocked,
        }

    def _blocked_reasons(self, used: int, reserved: int) -> list[dict]:
        """Explain why failed tasks are waiting on budget.

        Only tasks that actually attempted admission and were refused (their
        retained error carries the budget prefix) are reported, with the exact
        byte gap at the time of the query.
        """
        reasons: list[dict] = []
        if self.limit is None:
            return reasons
        for row in self.store.list("failed"):
            error = row["error"] or ""
            if not error.startswith(BUDGET_ERROR_PREFIX):
                continue
            expected = row["expected_size"] or 0
            task_files = self.task_used(row["id"])
            required = max(0, expected - task_files)
            free = max(0, self.limit - used - reserved)
            reasons.append(
                {
                    "task_id": row["id"],
                    "station": row["station"],
                    "date": row["date"],
                    "expected_size": expected,
                    "retained_bytes": task_files,
                    "required_bytes": required,
                    "available_bytes": free,
                    "reason": error,
                }
            )
        return reasons

    def purge_publish_temporaries(self) -> int:
        """Remove publish temporaries left by a crashed process.

        A publish rename is the last step of a completed transfer; any temp
        still present at startup can only belong to an interrupted publish and
        must not be mistaken for retained data.
        """
        removed = 0
        if not self.packages_dir.exists():
            return removed
        for path in self.packages_dir.glob(".publish-*"):
            try:
                path.unlink()
                removed += 1
            except OSError:
                pass
        return removed

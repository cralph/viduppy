import threading
from typing import List, Optional, Set


class QueueManager:
    """
    Thread-safe FIFO queue with priority controls, pause, and cancel support.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._queue: List[str] = []         # ordered pending job IDs
        self._paused: Set[str] = set()      # paused job IDs
        self._cancelled: Set[str] = set()   # cancelled job IDs
        self.active_job: Optional[str] = None
        self._stop_flag = threading.Event() # signal current job to stop

    # ── Queue management ──────────────────────────────────────────────────────

    def add_job(self, job_id: str):
        with self._lock:
            self._paused.discard(job_id)
            self._cancelled.discard(job_id)
            if job_id not in self._queue:
                self._queue.append(job_id)

    def get_queue(self) -> List[str]:
        with self._lock:
            return list(self._queue)

    def get_next_job(self) -> Optional[str]:
        """Return the next eligible job (not paused, not cancelled)."""
        with self._lock:
            for jid in self._queue:
                if jid not in self._paused and jid not in self._cancelled:
                    return jid
            return None

    def queue_position(self, job_id: str) -> int:
        """1-based position in queue, 0 if not in queue."""
        with self._lock:
            try:
                return self._queue.index(job_id) + 1
            except ValueError:
                return 0

    # ── Worker helpers ────────────────────────────────────────────────────────

    def start_processing(self, job_id: str):
        with self._lock:
            self.active_job = job_id
            self._stop_flag.clear()
            try:
                self._queue.remove(job_id)
            except ValueError:
                pass

    def finish_processing(self):
        with self._lock:
            self.active_job = None
            self._stop_flag.clear()

    def should_stop(self) -> bool:
        return self._stop_flag.is_set()

    # ── Controls ──────────────────────────────────────────────────────────────

    def pause_job(self, job_id: str) -> bool:
        with self._lock:
            self._paused.add(job_id)
            if self.active_job == job_id:
                self._stop_flag.set()
                # Re-insert at front so it resumes first when unpaused
                if job_id not in self._queue:
                    self._queue.insert(0, job_id)
                self.active_job = None
            return True

    def resume_job(self, job_id: str) -> bool:
        with self._lock:
            if job_id in self._paused:
                self._paused.discard(job_id)
                if job_id not in self._queue:
                    self._queue.insert(0, job_id)
                return True
            return False

    def cancel_job(self, job_id: str) -> bool:
        with self._lock:
            self._cancelled.add(job_id)
            if self.active_job == job_id:
                self._stop_flag.set()
                self.active_job = None
            try:
                self._queue.remove(job_id)
            except ValueError:
                pass
            return True

    def is_cancelled(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._cancelled

    # ── Priority ──────────────────────────────────────────────────────────────

    def change_priority(self, job_id: str, direction: str) -> bool:
        """
        direction: 'up' | 'down' | 'top' | 'bottom'
        """
        with self._lock:
            if job_id not in self._queue:
                return False
            idx = self._queue.index(job_id)
            q = self._queue
            if direction == 'up' and idx > 0:
                q[idx], q[idx - 1] = q[idx - 1], q[idx]
            elif direction == 'down' and idx < len(q) - 1:
                q[idx], q[idx + 1] = q[idx + 1], q[idx]
            elif direction == 'top':
                q.insert(0, q.pop(idx))
            elif direction == 'bottom':
                q.append(q.pop(idx))
            else:
                return False
            return True

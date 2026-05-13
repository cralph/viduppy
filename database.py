import sqlite3
import time
import config


def init_db():
    with sqlite3.connect(config.DATABASE) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                id              TEXT PRIMARY KEY,
                original_name   TEXT,
                filepath        TEXT,
                output_path     TEXT,
                scale           INTEGER,
                model           TEXT,
                start_frame     INTEGER DEFAULT 0,
                end_frame       INTEGER DEFAULT 0,
                total_frames    INTEGER DEFAULT 0,
                frames_extracted INTEGER DEFAULT 0,
                frames_to_process INTEGER DEFAULT 0,
                current_frame   INTEGER DEFAULT 0,
                fps             REAL DEFAULT 30,
                duration        REAL DEFAULT 0,
                width           INTEGER DEFAULT 0,
                height          INTEGER DEFAULT 0,
                output_factor   REAL DEFAULT 1.0,
                target_width    INTEGER DEFAULT 0,
                target_height   INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'queued',
                stage           TEXT DEFAULT 'En cola',
                progress        REAL DEFAULT 0,
                eta             INTEGER,
                elapsed_time    REAL DEFAULT 0,
                error_msg       TEXT,
                created_at      REAL,
                started_at      REAL,
                completed_at    REAL
            )
        ''')

        # Lightweight migration for existing DBs created before newer columns existed.
        cols = {
            row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        if 'elapsed_time' not in cols:
            conn.execute('ALTER TABLE jobs ADD COLUMN elapsed_time REAL DEFAULT 0')
        if 'output_factor' not in cols:
            conn.execute('ALTER TABLE jobs ADD COLUMN output_factor REAL DEFAULT 1.0')
        if 'target_width' not in cols:
            conn.execute('ALTER TABLE jobs ADD COLUMN target_width INTEGER DEFAULT 0')
        if 'target_height' not in cols:
            conn.execute('ALTER TABLE jobs ADD COLUMN target_height INTEGER DEFAULT 0')
        conn.commit()


def _row_to_dict(row):
    return dict(row) if row else None


def get_db():
    conn = sqlite3.connect(config.DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def get_all_jobs():
    with get_db() as conn:
        rows = conn.execute(
            'SELECT * FROM jobs ORDER BY created_at DESC'
        ).fetchall()
        return [_row_to_dict(r) for r in rows]


def get_job(job_id: str):
    with get_db() as conn:
        row = conn.execute(
            'SELECT * FROM jobs WHERE id = ?', (job_id,)
        ).fetchone()
        return _row_to_dict(row)


def create_job(data: dict):
    keys = [
        'id', 'original_name', 'filepath', 'scale', 'model',
        'start_frame', 'end_frame', 'total_frames', 'fps', 'duration',
        'width', 'height',
        'output_factor', 'target_width', 'target_height',
        'status', 'stage', 'progress', 'created_at',
    ]
    vals = [data.get(k) for k in keys]
    placeholders = ', '.join('?' * len(keys))
    col_names = ', '.join(keys)
    with get_db() as conn:
        conn.execute(
            f'INSERT INTO jobs ({col_names}) VALUES ({placeholders})', vals
        )
        conn.commit()
    return data


def update_job(job_id: str, updates: dict):
    if not updates:
        return
    sets = ', '.join(f'{k} = ?' for k in updates.keys())
    values = list(updates.values()) + [job_id]
    with get_db() as conn:
        conn.execute(f'UPDATE jobs SET {sets} WHERE id = ?', values)
        conn.commit()


def delete_job_record(job_id: str):
    with get_db() as conn:
        conn.execute('DELETE FROM jobs WHERE id = ?', (job_id,))
        conn.commit()

import os
import glob
import re as _re
import shutil
import subprocess
import time
from collections import deque

import config
from database import get_job, update_job
from queue_manager import QueueManager


class VideoProcessor:
    """Background worker that processes jobs from the queue."""

    def __init__(self, queue: QueueManager):
        self.queue = queue
        self._recent_frame_times: deque = deque(maxlen=20)

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self):
        while True:
            job_id = self.queue.get_next_job()
            if job_id:
                self.queue.start_processing(job_id)
                try:
                    self._process(job_id)
                except Exception as exc:
                    job = get_job(job_id) or {}
                    prev_elapsed = float(job.get('elapsed_time', 0) or 0)
                    started_at = float(job.get('started_at', 0) or 0)
                    elapsed_total = prev_elapsed + (time.time() - started_at if started_at else 0)
                    update_job(job_id, {
                        'status': 'error',
                        'stage': f'Error: {exc}',
                        'error_msg': str(exc),
                        'elapsed_time': round(elapsed_total, 1),
                        'completed_at': time.time(),
                        'eta': 0,
                    })
                finally:
                    self.queue.finish_processing()
            else:
                time.sleep(2)

    # ── High-level pipeline ───────────────────────────────────────────────────

    def _process(self, job_id: str):
        job = get_job(job_id)
        if not job or self.queue.is_cancelled(job_id):
            return

        frames_dir   = os.path.join(config.FRAMES_FOLDER,   job_id)
        upscaled_dir = os.path.join(config.UPSCALED_FOLDER, job_id)
        os.makedirs(frames_dir,   exist_ok=True)
        os.makedirs(upscaled_dir, exist_ok=True)

        # Per-job log file for debugging
        log_path = os.path.join(config.OUTPUT_FOLDER, f'{job_id}.log')

        process_started_at = time.time()
        base_elapsed = float(job.get('elapsed_time', 0) or 0)
        update_job(job_id, {'status': 'processing', 'started_at': process_started_at})

        keep_workdirs = False
        try:
            self._extract_frames(job_id, job, frames_dir, log_path)
            if self.queue.should_stop():
                keep_workdirs = not self.queue.is_cancelled(job_id)
                return
            use_upscayl = self._should_use_upscayl(job)
            source_dir = frames_dir
            if use_upscayl:
                upscayl_factor = self._effective_upscayl_factor(job)
                self._upscale_frames(
                    job_id, job, frames_dir, upscaled_dir, log_path,
                    upscayl_factor=upscayl_factor,
                )
                if self.queue.should_stop():
                    keep_workdirs = not self.queue.is_cancelled(job_id)
                    return
                source_dir = upscaled_dir
            else:
                upscayl_factor = 1
                out_w, out_h = self._desired_output_size(job)
                self._log(
                    log_path,
                    f'Skipping upscayl: downsize/same-size target ({out_w}x{out_h}) '
                    f'<= source ({job.get("width", 0)}x{job.get("height", 0)}).'
                )
                update_job(job_id, {
                    'stage': 'Resizing with FFmpeg (without Upscayl)…',
                    'progress': 85,
                    'eta': 0,
                })
            self._assemble_video(
                job_id,
                job,
                source_dir,
                log_path,
                used_upscayl=use_upscayl,
                upscayl_factor=upscayl_factor,
                process_started_at=process_started_at,
                base_elapsed=base_elapsed,
            )
        finally:
            # Keep temporary dirs when paused so resume can continue from checkpoints.
            if keep_workdirs:
                self._log(log_path, 'Paused: keeping frames/upscaled dirs for resume.')
            else:
                shutil.rmtree(frames_dir,   ignore_errors=True)
                shutil.rmtree(upscaled_dir, ignore_errors=True)

    # ── Step 1 – Extract frames ───────────────────────────────────────────────

    def _extract_frames(self, job_id: str, job: dict, frames_dir: str, log_path: str):
        fps         = job['fps']
        start_frame = job['start_frame']
        end_frame   = job['end_frame']
        start_time  = start_frame / fps
        duration    = (end_frame - start_frame) / fps
        total_exp   = end_frame - start_frame
        existing    = self._count_pngs(frames_dir)

        if existing >= total_exp > 0:
            self._log(
                log_path,
                f'Extraction skipped: reusing {existing} existing frames (expected {total_exp}).'
            )
            update_job(job_id, {
                'stage': 'Reusing extracted frames',
                'progress': 12,
                'frames_extracted': existing,
                'frames_to_process': existing,
            })
            return

        update_job(job_id, {
            'stage':    'Extracting frames',
            'progress': 0,
            'frames_to_process': total_exp,
        })

        self._log(log_path, f'=== EXTRACT FRAMES ===')
        self._log(log_path, f'start_time={start_time:.4f}s  duration={duration:.4f}s  expected={total_exp}')

        ffmpeg_exec = config.FFMPEG_BIN or 'ffmpeg'
        ffmpeg_log = os.path.join(frames_dir, '_ffmpeg_extract.log')
        cmd = [
            ffmpeg_exec, '-y',
            '-ss', f'{start_time:.4f}',
            '-i', job['filepath'],
            '-t', f'{duration:.4f}',
            '-vf', f'fps={fps}',
            '-q:v', '2',
            os.path.join(frames_dir, 'frame_%06d.png'),
        ]
        self._log(log_path, 'CMD: ' + ' '.join(cmd))

        with open(ffmpeg_log, 'w') as log_f:
            proc = subprocess.Popen(cmd, stdout=log_f, stderr=subprocess.STDOUT)

        while proc.poll() is None:
            if self.queue.should_stop():
                proc.terminate()
                return
            count = len(glob.glob(os.path.join(frames_dir, '*.png')))
            pct   = min(100, (count / max(total_exp, 1)) * 100)
            update_job(job_id, {
                'stage':    f'Extracting frames ({count}/{total_exp})',
                'progress': round(pct * 0.12, 1),
                'frames_extracted': count,
            })
            time.sleep(0.5)

        proc.wait()
        if proc.returncode != 0:
            try:
                log_tail = open(ffmpeg_log).read()[-800:]
            except Exception:
                log_tail = '(no log)'
            raise RuntimeError(f'FFmpeg extraction failed (code {proc.returncode}):\n{log_tail}')

        extracted = len(glob.glob(os.path.join(frames_dir, '*.png')))
        self._log(log_path, f'Extracted {extracted} frames OK')
        update_job(job_id, {
            'stage':    f'Frames extracted: {extracted}',
            'progress': 12,
            'frames_extracted': extracted,
            'frames_to_process': extracted,
        })

    # ── Step 2 – Upscale frames ───────────────────────────────────────────────

    def _upscale_frames(self, job_id: str, job: dict,
                        frames_dir: str, upscaled_dir: str, log_path: str,
                        upscayl_factor: int):

        all_frame_names = sorted(
            f for f in os.listdir(frames_dir)
            if f.endswith('.png') and not f.startswith('._')
        )
        total = len(all_frame_names)
        if total == 0:
            raise RuntimeError('No frames were extracted.')

        if not config.UPSCAYL_BIN:
            raise RuntimeError('upscayl-bin not found. Set the path in Settings.')
        if not config.UPSCAYL_MODELS_DIR:
            raise RuntimeError('Models directory not found. Set the path in Settings.')
        if not self._model_is_installed(job['model']):
            installed = ', '.join(self._list_installed_models()[:12]) or '(none)'
            raise RuntimeError(
                f'Model "{job["model"]}" is not installed in {config.UPSCAYL_MODELS_DIR}. '
                f'Detected models: {installed}'
            )

        self._log(log_path, f'\n=== UPSCALE FRAMES ({total} frames) ===')
        self._log(log_path, f'binary={config.UPSCAYL_BIN}')
        self._log(log_path, f'models={config.UPSCAYL_MODELS_DIR}')
        self._log(
            log_path,
            f'scale_requested={job["scale"]}  scale_effective={upscayl_factor}  model={job["model"]}',
        )
        if int(job.get('scale', 1) or 1) != int(upscayl_factor):
            self._log(
                log_path,
                'Model-native scale override enabled. Final target size will still follow requested output settings.',
            )

        # ── Determine which frames still need upscaling ───────────────────────
        # Strip upscayl suffix (e.g. frame_000001_upscayl_4x_model.png → frame_000001.png)
        def _normalize(name: str) -> str:
            m = _re.match(r'(frame_\d+)', name)
            return (m.group(1) + '.png') if m else name

        upscaled_done = {
            _normalize(f)
            for f in os.listdir(upscaled_dir)
            if f.endswith('.png') and not f.startswith('._')
        }
        needed = [f for f in all_frame_names if f not in upscaled_done]

        # On Windows, prefer correctness over resume-cache reuse:
        # previous corrupted PNGs can be reused and keep producing mosaic output.
        # Rebuild the upscaled set from scratch.
        if os.name == 'nt' and upscaled_done:
            self._log(
                log_path,
                'Windows safe mode: ignoring cached upscaled PNGs and rebuilding frame set from scratch.',
            )
            for path in glob.glob(os.path.join(upscaled_dir, '*.png')):
                try:
                    os.remove(path)
                except OSError:
                    pass
            needed = list(all_frame_names)

        if not needed:
            self._log(log_path, 'All frames already upscaled, skipping upscayl-bin.')
        else:
            # Windows-safe mode: process each frame independently.
            # Some upscayl-bin Windows builds can produce tile-mixed outputs in
            # directory batch mode; per-frame mode is slower but stable.
            if os.name == 'nt':
                self._upscale_frames_serial_windows(
                    job_id=job_id,
                    job=job,
                    needed=needed,
                    total=total,
                    frames_dir=frames_dir,
                    upscaled_dir=upscaled_dir,
                    log_path=log_path,
                    upscayl_factor=upscayl_factor,
                )
            else:
                self._upscale_frames_batch_directory(
                    job_id=job_id,
                    job=job,
                    needed=needed,
                    total=total,
                    frames_dir=frames_dir,
                    upscaled_dir=upscaled_dir,
                    log_path=log_path,
                    upscayl_factor=upscayl_factor,
                )

        # ── Normalize and sanitize output frame set ───────────────────────────
        # Some upscayl-bin builds can emit extra variant/tile-like PNGs.
        # Keep exactly one candidate per frame index, then rebuild a clean
        # frame_000001.png..frame_%06d.png set for FFmpeg.
        self._log(log_path, 'Post-upscale: normalizing output frame set…')
        frame_candidates: dict[int, list[tuple[str, int]]] = {}
        ignored_pngs = 0
        with os.scandir(upscaled_dir) as scan:
            for entry in scan:
                if not (entry.is_file() and entry.name.endswith('.png') and not entry.name.startswith('._')):
                    continue
                m = _re.match(r'^frame_(\d+)', entry.name)
                if not m:
                    ignored_pngs += 1
                    continue
                idx = int(m.group(1))
                try:
                    fsize = entry.stat().st_size
                except OSError:
                    fsize = 0
                frame_candidates.setdefault(idx, []).append((entry.path, fsize))

        if ignored_pngs:
            self._log(log_path, f'Ignored {ignored_pngs} non-frame PNG(s) in upscaled_dir.')

        missing = [i for i in range(1, total + 1) if i not in frame_candidates]
        if missing:
            sample = ', '.join(str(i) for i in missing[:12])
            raise RuntimeError(
                f'Missing upscaled frames after normalization ({len(missing)} missing). '
                f'First missing indices: {sample}. Check log: {log_path}'
            )

        chosen_paths: list[tuple[int, str]] = []
        variant_frames = 0
        for idx in range(1, total + 1):
            candidates = frame_candidates[idx]
            if len(candidates) > 1:
                variant_frames += 1
            # Choose the largest candidate (usually the full stitched frame).
            best_path, _ = max(candidates, key=lambda it: (it[1], it[0]))
            chosen_paths.append((idx, best_path))

        if variant_frames:
            self._log(
                log_path,
                f'Found multiple PNG variants for {variant_frames} frame index(es); '
                'selected the largest file per index.',
            )

        normalized_dir = os.path.join(upscaled_dir, '_normalized_frames')
        shutil.rmtree(normalized_dir, ignore_errors=True)
        os.makedirs(normalized_dir, exist_ok=True)
        try:
            for idx, src in chosen_paths:
                dst = os.path.join(normalized_dir, f'frame_{idx:06d}.png')
                shutil.copy2(src, dst)

            normalized_pngs = sorted(
                f for f in os.listdir(normalized_dir)
                if f.endswith('.png') and not f.startswith('._')
            )
            if len(normalized_pngs) != total:
                raise RuntimeError(
                    f'Normalized frame count mismatch: got {len(normalized_pngs)}, expected {total}.'
                )

            # Optional sanity check: resolution should be close to source*scale.
            sample_path = os.path.join(normalized_dir, normalized_pngs[0])
            probe_w, probe_h = self._probe_image_size(sample_path)
            exp_w = int(job.get('width', 0) or 0) * int(job.get('scale', 1) or 1)
            exp_h = int(job.get('height', 0) or 0) * int(job.get('scale', 1) or 1)
            if probe_w > 0 and probe_h > 0 and exp_w > 0 and exp_h > 0:
                if abs(probe_w - exp_w) > 16 or abs(probe_h - exp_h) > 16:
                    self._log(
                        log_path,
                        f'WARNING: unexpected upscaled frame size {probe_w}x{probe_h} '
                        f'(expected around {exp_w}x{exp_h}).',
                    )

            # Rebuild clean set in upscaled_dir.
            for path in glob.glob(os.path.join(upscaled_dir, '*.png')):
                try:
                    os.remove(path)
                except OSError:
                    pass
            for fname in normalized_pngs:
                self._replace_with_retry(
                    os.path.join(normalized_dir, fname),
                    os.path.join(upscaled_dir, fname),
                )
        finally:
            shutil.rmtree(normalized_dir, ignore_errors=True)

        final_pngs = sorted(
            f for f in os.listdir(upscaled_dir)
            if f.endswith('.png') and not f.startswith('._')
        )
        self._log(log_path, f'After normalization: {len(final_pngs)} PNGs in upscaled_dir')
        if len(final_pngs) != total:
            raise RuntimeError(
                f'Final upscaled frame count mismatch: {len(final_pngs)} vs expected {total}. '
                f'Check log: {log_path}'
            )

        # ── Black-frame sanity check ──────────────────────────────────────────
        first_png = os.path.join(upscaled_dir, final_pngs[0])
        if self._is_black_frame(first_png):
            self._log(log_path,
                f'WARNING: First upscaled frame is fully black. '
                f'Model "{job["model"]}" may not be installed '
                f'({job["model"]}.param or {job["model"]}.bin missing). '
                f'Select an installed model in Settings.'
            )
            raise RuntimeError(
                f'Upscaled frames are black. '
                f'Model "{job["model"]}" is likely not installed. '
                f'Ensure {job["model"]}.param and {job["model"]}.bin exist '
                f'in the models directory and select a valid model.'
            )

        self._log(log_path, f'Upscaling done. {total} frames ready.')
        update_job(job_id, {'stage': 'Frames upscaled, assembling video…', 'progress': 85, 'eta': 0})

    def _upscale_frames_batch_directory(self, job_id: str, job: dict, needed: list[str],
                                        total: int, frames_dir: str, upscaled_dir: str,
                                        log_path: str, upscayl_factor: int):
        subset_dir = os.path.join(os.path.dirname(frames_dir), f'subset_{job_id[:8]}')
        shutil.rmtree(subset_dir, ignore_errors=True)
        os.makedirs(subset_dir)

        try:
            for fname in needed:
                src = os.path.join(frames_dir, fname)
                dst = os.path.join(subset_dir, fname)
                try:
                    os.link(src, dst)
                except OSError:
                    shutil.copy2(src, dst)

            bin_dir = os.path.dirname(os.path.abspath(config.UPSCAYL_BIN))
            try:
                models_arg = os.path.relpath(config.UPSCAYL_MODELS_DIR, bin_dir)
            except ValueError:
                models_arg = config.UPSCAYL_MODELS_DIR
            self._log(
                log_path,
                f'bin_dir={bin_dir}  models_abs={config.UPSCAYL_MODELS_DIR}  models_rel={models_arg}',
            )

            scale_args = self._upscayl_scale_args(upscayl_factor, log_path)
            cmd_base = [
                config.UPSCAYL_BIN,
                '-i', subset_dir,
                '-o', upscaled_dir,
                '-m', models_arg,
                '-n', job['model'],
                '-f', 'png',
            ] + scale_args
            gpu_flags = self._upscayl_gpu_flags(log_path)
            cmd_preferred = cmd_base + (gpu_flags or [])
            cmd_abs_models = [
                config.UPSCAYL_BIN,
                '-i', subset_dir,
                '-o', upscaled_dir,
                '-m', config.UPSCAYL_MODELS_DIR,
                '-n', job['model'],
                '-f', 'png',
            ] + scale_args

            self._log(log_path, 'CMD: ' + ' '.join(cmd_preferred))
            update_job(job_id, {
                'stage': f'Upscaling {len(needed)} frames…',
                'progress': 12,
                'frames_to_process': total,
            })

            def _run_upscayl(run_cmd: list, label: str):
                self._log(log_path, f'CMD [{label}]: ' + ' '.join(run_cmd))
                upscayl_log = os.path.join(config.OUTPUT_FOLDER, f'{job_id}_upscayl_{label}.log')
                log_f = open(upscayl_log, 'a', encoding='utf-8', errors='replace')
                p = subprocess.Popen(
                    run_cmd,
                    stdout=log_f,
                    stderr=subprocess.STDOUT,
                    text=True,
                )

                _start_t = time.time()
                _start_done = min(total, self._count_pngs(upscaled_dir))
                _last_done = -1

                def _update_from_counts(force: bool = False):
                    nonlocal _last_done
                    done = min(total, self._count_pngs(upscaled_dir))
                    pct = (done / max(total, 1)) * 100
                    now = time.time()
                    if force or done != _last_done:
                        _last_done = done
                        overall = 12 + pct * 0.73
                        processed = max(0, done - _start_done)
                        remaining = max(0, total - done)
                        elapsed = max(0.001, now - _start_t)
                        eta = int((elapsed / processed) * remaining) if processed > 0 else 0
                        update_job(job_id, {
                            'stage': f'Upscaling frames ({done}/{total}) · {pct:.1f}%',
                            'progress': round(overall, 1),
                            'current_frame': done,
                            'frames_to_process': total,
                            'eta': eta,
                        })

                _update_from_counts(force=True)
                try:
                    while p.poll() is None:
                        if self.queue.should_stop():
                            p.terminate()
                            try:
                                p.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                p.kill()
                                p.wait()
                            return p, upscayl_log
                        time.sleep(0.5)
                        _update_from_counts()
                finally:
                    log_f.close()

                _update_from_counts(force=True)
                p.wait()
                self._log(log_path, f'upscayl [{label}] exit_code={p.returncode}')
                return p, upscayl_log

            if config.FORCE_CPU:
                self._log(
                    log_path,
                    'FORCE_CPU=True but this upscayl-bin build does not support -g -1 (CPU). Running on GPU anyway.',
                )

            proc, upscayl_log = _run_upscayl(cmd_preferred, 'gpu')
            if self.queue.should_stop():
                return

            if proc.returncode in (3221225477, -1073741819) and gpu_flags:
                self._log(
                    log_path,
                    f'upscayl-bin exited with access violation ({proc.returncode}). Retrying once without explicit GPU device flags.',
                )
                time.sleep(1.0)
                proc, upscayl_log = _run_upscayl(cmd_base, 'gpu-retry')
                if self.queue.should_stop():
                    return

            if proc.returncode in (3221225477, -1073741819):
                self._log(
                    log_path,
                    f'upscayl-bin still failing ({proc.returncode}). Retrying once with absolute models path.',
                )
                time.sleep(1.0)
                proc, upscayl_log = _run_upscayl(cmd_abs_models, 'gpu-retry-abs-models')
                if self.queue.should_stop():
                    return

            if proc.returncode != 0:
                raise RuntimeError(
                    self._format_upscayl_failure(
                        job=job,
                        return_code=proc.returncode,
                        upscayl_log=upscayl_log,
                        main_log=log_path,
                    )
                )
        finally:
            shutil.rmtree(subset_dir, ignore_errors=True)

    def _upscale_frames_serial_windows(self, job_id: str, job: dict, needed: list[str],
                                       total: int, frames_dir: str, upscaled_dir: str,
                                       log_path: str, upscayl_factor: int):
        self._log(
            log_path,
            f'Windows safe mode enabled: running upscayl-bin per-frame for {len(needed)} frame(s).',
        )
        update_job(job_id, {
            'stage': f'Upscaling {len(needed)} frames (Windows safe mode)…',
            'progress': 12,
            'frames_to_process': total,
        })

        gpu_flags = self._upscayl_gpu_flags(log_path)
        bin_dir = os.path.dirname(os.path.abspath(config.UPSCAYL_BIN))
        try:
            models_arg = os.path.relpath(config.UPSCAYL_MODELS_DIR, bin_dir)
        except ValueError:
            models_arg = config.UPSCAYL_MODELS_DIR
        self._log(
            log_path,
            f'bin_dir={bin_dir}  models_abs={config.UPSCAYL_MODELS_DIR}  models_rel={models_arg}',
        )

        start_t = time.time()
        for idx, fname in enumerate(needed, start=1):
            if self.queue.should_stop():
                return

            src = os.path.join(frames_dir, fname)
            dst = os.path.join(upscaled_dir, fname)
            scale_args = self._upscayl_scale_args(upscayl_factor, log_path)
            cmd_base = [
                config.UPSCAYL_BIN,
                '-i', src,
                '-o', dst,
                '-m', models_arg,
                '-n', job['model'],
                '-f', 'png',
            ] + scale_args
            cmd_preferred = cmd_base + (gpu_flags or [])
            cmd_abs_models = [
                config.UPSCAYL_BIN,
                '-i', src,
                '-o', dst,
                '-m', config.UPSCAYL_MODELS_DIR,
                '-n', job['model'],
                '-f', 'png',
            ] + scale_args
            frame_label = f'frame_{idx:06d}'
            rc, upscayl_log = self._run_upscayl_once(
                run_cmd=cmd_preferred,
                job_id=job_id,
                label=f'{frame_label}_gpu',
                log_path=log_path,
            )
            if self.queue.should_stop():
                return
            if rc in (3221225477, -1073741819) and gpu_flags:
                self._log(
                    log_path,
                    f'[{fname}] access violation ({rc}). Retrying without explicit GPU device flags.',
                )
                rc, upscayl_log = self._run_upscayl_once(
                    run_cmd=cmd_base,
                    job_id=job_id,
                    label=f'{frame_label}_gpu_retry',
                    log_path=log_path,
                )
                if self.queue.should_stop():
                    return
            if rc in (3221225477, -1073741819):
                self._log(
                    log_path,
                    f'[{fname}] still failing ({rc}). Retrying with absolute models path.',
                )
                rc, upscayl_log = self._run_upscayl_once(
                    run_cmd=cmd_abs_models,
                    job_id=job_id,
                    label=f'{frame_label}_gpu_retry_abs_models',
                    log_path=log_path,
                )
                if self.queue.should_stop():
                    return
            if rc != 0:
                raise RuntimeError(
                    self._format_upscayl_failure(
                        job=job,
                        return_code=rc,
                        upscayl_log=upscayl_log,
                        main_log=log_path,
                    )
                )

            # Some builds ignore output filename and append suffixes.
            if not os.path.exists(dst):
                prefix = os.path.splitext(fname)[0]
                candidates = sorted(
                    p for p in glob.glob(os.path.join(upscaled_dir, f'{prefix}*.png'))
                    if os.path.isfile(p)
                )
                if candidates:
                    best = max(candidates, key=lambda p: os.path.getsize(p))
                    self._replace_with_retry(best, dst)

            done = min(total, self._count_pngs(upscaled_dir))
            pct = (done / max(total, 1)) * 100
            overall = 12 + pct * 0.73
            processed = idx
            remaining = max(0, len(needed) - idx)
            elapsed = max(0.001, time.time() - start_t)
            eta = int((elapsed / processed) * remaining) if processed > 0 else 0
            update_job(job_id, {
                'stage': f'Upscaling frames ({done}/{total}) · {pct:.1f}% · Windows safe mode',
                'progress': round(overall, 1),
                'current_frame': done,
                'frames_to_process': total,
                'eta': eta,
            })

    def _run_upscayl_once(self, run_cmd: list, job_id: str, label: str,
                          log_path: str) -> tuple[int, str]:
        self._log(log_path, f'CMD [{label}]: ' + ' '.join(run_cmd))
        upscayl_log = os.path.join(config.OUTPUT_FOLDER, f'{job_id}_upscayl_{label}.log')
        with open(upscayl_log, 'a', encoding='utf-8', errors='replace') as log_f:
            p = subprocess.Popen(
                run_cmd,
                stdout=log_f,
                stderr=subprocess.STDOUT,
                text=True,
            )
            while p.poll() is None:
                if self.queue.should_stop():
                    p.terminate()
                    try:
                        p.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        p.kill()
                        p.wait()
                    break
                time.sleep(0.2)
            p.wait()
        self._log(log_path, f'upscayl [{label}] exit_code={p.returncode}')
        return p.returncode, upscayl_log

    # ── Step 3 – Reassemble video ─────────────────────────────────────────────

    def _assemble_video(self, job_id: str, job: dict, frames_input_dir: str,
                        log_path: str, used_upscayl: bool, upscayl_factor: int,
                        process_started_at: float, base_elapsed: float):
        update_job(job_id, {'stage': 'Assembling final video', 'progress': 87})
        self._log(log_path, '\n=== ASSEMBLE VIDEO ===')

        safe_model = job['model'].replace('/', '_')
        out_suffix = self._output_suffix(job)
        out_name   = f"upscaled_{job['id'][:8]}_{job['scale']}x_{safe_model}{out_suffix}.mp4"
        out_path   = os.path.join(config.OUTPUT_FOLDER, out_name)
        fps        = job['fps']
        has_audio  = self._has_audio(job['filepath'])

        frame_pattern = os.path.join(frames_input_dir, 'frame_%06d.png')

        # Verify we actually have frames before trying to encode
        png_count = len(glob.glob(os.path.join(frames_input_dir, '*.png')))
        self._log(log_path, f'fps={fps}  has_audio={has_audio}  png_count={png_count}  out={out_path}')
        if png_count == 0:
            raise RuntimeError('No frames available to assemble the video.')

        # ── Choose encoder: NVENC (GPU) or libx264 (CPU fallback) ────────────
        use_nvenc = config.USE_NVENC and self._nvenc_available()
        encoder_args = (
            ['-c:v', 'h264_nvenc', '-preset', 'p4', '-rc', 'vbr', '-cq', '18',
             '-b:v', '0', '-pix_fmt', 'yuv420p']
            if use_nvenc else
            ['-c:v', 'libx264', '-preset', 'medium', '-crf', '18', '-pix_fmt', 'yuv420p']
        )
        encoder_name = 'h264_nvenc' if use_nvenc else 'libx264'
        self._log(log_path, f'encoder={encoder_name}')

        scale_filter = self._build_output_scale_filter(job, used_upscayl, upscayl_factor)
        if scale_filter:
            self._log(log_path, f'output_scale_filter={scale_filter}')

        ffmpeg_exec = config.FFMPEG_BIN or 'ffmpeg'
        encode_base = [
            ffmpeg_exec, '-y',
            '-start_number', '1',      # explicit: frames are frame_000001.png…
            '-framerate', str(fps),
            '-i', frame_pattern,
        ]
        if scale_filter:
            encode_base += ['-vf', scale_filter]
        encode_base += encoder_args

        if has_audio:
            start_t  = job['start_frame'] / fps
            duration = (job['end_frame'] - job['start_frame']) / fps
            tmp_path = out_path.replace('.mp4', '_novid.mp4')

            update_job(job_id, {
                'stage':    'Assembling final video · encoding video',
                'progress': 90,
                'eta':       0,
            })
            self._run_log(encode_base + [tmp_path], log_path)

            update_job(job_id, {
                'stage':    'Assembling final video · muxing audio',
                'progress': 95,
                'eta':       0,
            })
            self._run_log([
                ffmpeg_exec, '-y',
                '-i', tmp_path,
                '-ss', f'{start_t:.4f}', '-t', f'{duration:.4f}',
                '-i', job['filepath'],
                '-c:v', 'copy', '-c:a', 'aac',
                '-map', '0:v:0', '-map', '1:a:0',
                '-shortest', out_path,
            ], log_path)
            os.remove(tmp_path)
        else:
            update_job(job_id, {
                'stage':    'Assembling final video · encoding video',
                'progress': 90,
                'eta':       0,
            })
            self._run_log(encode_base + [out_path], log_path)

        self._log(log_path, f'Assembly complete: {out_path}')
        total_elapsed = base_elapsed + max(0.0, time.time() - process_started_at)
        update_job(job_id, {
            'status':       'completed',
            'stage':        'Completed ✓',
            'progress':     100,
            'output_path':  out_path,
            'completed_at': time.time(),
            'elapsed_time': round(total_elapsed, 1),
            'eta':          0,
        })

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _run_log(self, cmd: list, log_path: str):
        self._log(log_path, 'CMD: ' + ' '.join(str(c) for c in cmd))
        result = subprocess.run(cmd, capture_output=True)
        if result.returncode != 0:
            err = result.stderr.decode(errors='replace')[-600:]
            self._log(log_path, f'ERROR: {err}')
            raise RuntimeError(err)

    def _run(self, cmd: list):
        result = subprocess.run(cmd, capture_output=True)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.decode(errors='replace')[-500:])

    def _upscayl_gpu_flags(self, log_path: str) -> list[str]:
        """Return GPU selection flags for upscayl-bin when its build supports them."""
        if not config.UPSCAYL_BIN:
            return []
        try:
            r = subprocess.run(
                [config.UPSCAYL_BIN, '--help'],
                capture_output=True, text=True, timeout=8,
            )
            help_text = (r.stdout or '') + (r.stderr or '')
        except Exception as exc:
            self._log(log_path, f'Could not inspect upscayl-bin GPU flags: {exc}')
            return []

        supports_gpu_arg = bool(_re.search(r'(^|\s)-g([,\s]|$)|gpu', help_text, _re.I))
        if not supports_gpu_arg:
            self._log(log_path, 'upscayl-bin help does not advertise -g; using default GPU selection.')
            return []

        if config.FORCE_CPU:
            self._log(log_path, 'FORCE_CPU=True: passing -g -1 to upscayl-bin.')
            return ['-g', '-1']

        gpu_id = int(getattr(config, 'GPU_DEVICE', 0) or 0)
        self._log(log_path, f'Using upscayl-bin GPU device {gpu_id} via -g.')
        return ['-g', str(gpu_id)]

    def _upscayl_scale_args(self, factor: int, log_path: str) -> list[str]:
        """
        Pick the scale flag supported by this upscayl-bin build.
        Prefer -s (current CLI), fallback to -z for older builds.
        """
        f = max(1, int(factor or 1))
        if not config.UPSCAYL_BIN:
            return ['-s', str(f)]
        try:
            r = subprocess.run(
                [config.UPSCAYL_BIN, '--help'],
                capture_output=True, text=True, timeout=8,
            )
            help_text = (r.stdout or '') + (r.stderr or '')
        except Exception as exc:
            self._log(log_path, f'Could not inspect upscayl-bin scale flag: {exc}')
            return ['-s', str(f)]

        has_s = bool(_re.search(r'(^|\s)-s([,\s]|$)|scale', help_text, _re.I))
        has_z = bool(_re.search(r'(^|\s)-z([,\s]|$)', help_text, _re.I))
        if has_s:
            return ['-s', str(f)]
        if has_z:
            return ['-z', str(f)]

        # Safe default for modern upscayl-bin.
        return ['-s', str(f)]

    def _is_black_frame(self, png_path: str) -> bool:
        """Downscale PNG to 1×1 via ffmpeg and check if average colour is near black."""
        ffmpeg_exec = config.FFMPEG_BIN or 'ffmpeg'
        try:
            r = subprocess.run(
                [ffmpeg_exec, '-loglevel', 'error',
                 '-i', png_path,
                 '-vf', 'scale=1:1',
                 '-vframes', '1',
                 '-f', 'rawvideo', '-pix_fmt', 'rgb24', '-'],
                capture_output=True, timeout=10,
            )
            if r.returncode != 0 or len(r.stdout) < 3:
                return False
            avg = sum(r.stdout[:3]) / 3
            return avg < 8          # threshold: average channel < 8/255 ≈ very dark
        except Exception:
            return False

    def _nvenc_available(self) -> bool:
        """Quick test — encode 1 null frame with h264_nvenc; returns True if it works."""
        ffmpeg_exec = config.FFMPEG_BIN or 'ffmpeg'
        try:
            r = subprocess.run(
                [ffmpeg_exec, '-loglevel', 'error',
                 '-f', 'lavfi', '-i', 'nullsrc=s=256x256:d=0.1',
                 '-c:v', 'h264_nvenc', '-f', 'null', '-'],
                capture_output=True, timeout=10,
            )
            return r.returncode == 0
        except Exception:
            return False

    def _has_audio(self, filepath: str) -> bool:
        ffprobe_exec = config.FFPROBE_BIN or 'ffprobe'
        r = subprocess.run(
            [ffprobe_exec, '-v', 'quiet', '-show_streams',
             '-select_streams', 'a', filepath],
            capture_output=True, text=True,
        )
        return 'codec_type=audio' in r.stdout

    def _probe_image_size(self, image_path: str) -> tuple[int, int]:
        ffprobe_exec = config.FFPROBE_BIN or 'ffprobe'
        try:
            r = subprocess.run(
                [
                    ffprobe_exec, '-v', 'error',
                    '-select_streams', 'v:0',
                    '-show_entries', 'stream=width,height',
                    '-of', 'csv=p=0:s=x',
                    image_path,
                ],
                capture_output=True, text=True, timeout=8,
            )
            if r.returncode != 0:
                return 0, 0
            txt = (r.stdout or '').strip()
            if 'x' not in txt:
                return 0, 0
            w_s, h_s = txt.split('x', 1)
            return int(w_s or 0), int(h_s or 0)
        except Exception:
            return 0, 0

    def _calc_eta(self, frames_remaining: int) -> int:
        if not self._recent_frame_times:
            return 0
        avg = sum(self._recent_frame_times) / len(self._recent_frame_times)
        return int(avg * frames_remaining)

    def _count_pngs(self, directory: str) -> int:
        """Count PNG files with shell-equivalent semantics: ls <dir>/*.png | wc -l."""
        return len(glob.glob(os.path.join(directory, '*.png')))

    def _build_output_scale_filter(self, job: dict, used_upscayl: bool,
                                   upscayl_factor: int) -> str:
        """Build optional FFmpeg scale filter for final output sizing."""
        out_w, out_h = self._desired_output_size(job)
        in_w = int(job.get('width', 0) or 0)
        in_h = int(job.get('height', 0) or 0)
        if used_upscayl:
            in_w *= max(1, int(upscayl_factor or 1))
            in_h *= max(1, int(upscayl_factor or 1))

        if out_w > 0 and out_h > 0 and (out_w != in_w or out_h != in_h):
            return f'scale={out_w}:{out_h}'
        return ''

    def _desired_output_size(self, job: dict) -> tuple[int, int]:
        """Compute final output size from scale + optional final resize inputs."""
        src_w = int(job.get('width', 0) or 0)
        src_h = int(job.get('height', 0) or 0)
        scale = max(1, int(job.get('scale', 1) or 1))
        factor = float(job.get('output_factor', 1.0) or 1.0)
        if factor <= 0:
            factor = 1.0
        target_w = int(job.get('target_width', 0) or 0)
        target_h = int(job.get('target_height', 0) or 0)

        base_w = src_w * scale
        base_h = src_h * scale

        def _even(n: int) -> int:
            n = max(2, int(n))
            return n - (n % 2)

        if target_w > 0 and target_h > 0:
            return _even(target_w), _even(target_h)
        if target_w > 0:
            out_w = _even(target_w)
            out_h = _even(round(base_h * out_w / max(base_w, 1)))
            return out_w, out_h
        if target_h > 0:
            out_h = _even(target_h)
            out_w = _even(round(base_w * out_h / max(base_h, 1)))
            return out_w, out_h
        if abs(factor - 1.0) > 1e-6:
            return _even(round(base_w * factor)), _even(round(base_h * factor))
        return max(base_w, 0), max(base_h, 0)

    def _should_use_upscayl(self, job: dict) -> bool:
        """Use upscayl only if final target is larger than the source dimensions."""
        out_w, out_h = self._desired_output_size(job)
        src_w = int(job.get('width', 0) or 0)
        src_h = int(job.get('height', 0) or 0)
        if src_w <= 0 or src_h <= 0 or out_w <= 0 or out_h <= 0:
            return True
        return out_w > src_w or out_h > src_h

    def _effective_upscayl_factor(self, job: dict) -> int:
        """
        Derive the upscayl model's native factor from its name.
        Many default models are x4-only; requesting x2/x3 on those can be unstable.
        """
        requested = max(1, int(job.get('scale', 1) or 1))
        model = str(job.get('model', '') or '').lower()

        if 'x4' in model or '4x' in model:
            return 4
        if 'x3' in model or '3x' in model:
            return 3
        if 'x2' in model or '2x' in model:
            return 2
        return requested

    def _output_suffix(self, job: dict) -> str:
        target_w = int(job.get('target_width', 0) or 0)
        target_h = int(job.get('target_height', 0) or 0)
        factor   = float(job.get('output_factor', 1.0) or 1.0)
        if target_w > 0 and target_h > 0:
            return f'_{target_w}x{target_h}'
        if target_w > 0:
            return f'_w{target_w}'
        if target_h > 0:
            return f'_h{target_h}'
        if abs(factor - 1.0) > 1e-6:
            return f'_f{factor:g}'
        return ''

    def _format_upscayl_failure(self, job: dict, return_code: int,
                                upscayl_log: str, main_log: str) -> str:
        if return_code in (3221225477, -1073741819):
            tail = self._tail_text(upscayl_log, 500)
            details = f' Last upscayl log tail: {tail}' if tail else ''
            return (
                f'Upscayl-bin crashed with Windows access violation (code {return_code}, 0xC0000005). '
                'This is usually a GPU/driver/runtime crash (often after pause/resume), not a missing model. '
                'Try changing GPU device in Settings, disabling overlays/recorders, or updating GPU drivers. '
                f'Check logs: {main_log} and {upscayl_log}.{details}'
            )

        return (
            f'Upscayl-bin failed (code {return_code}). '
            f'Check that model "{job["model"]}" is installed '
            f'(requires {job["model"]}.param and {job["model"]}.bin in the models directory). '
            f'Check logs: {main_log} and {upscayl_log}.'
        )

    def _tail_text(self, path: str, limit: int = 500) -> str:
        try:
            txt = open(path, encoding='utf-8', errors='replace').read()
            txt = txt.strip()
            return txt[-limit:] if txt else ''
        except Exception:
            return ''

    def _replace_with_retry(self, src: str, dst: str, retries: int = 8):
        """
        Robust replace for Windows/OneDrive paths where files may be briefly locked
        by antivirus/indexer/sync.
        """
        last_exc = None
        for i in range(retries):
            try:
                if os.path.exists(dst):
                    os.remove(dst)
                os.replace(src, dst)
                return
            except FileNotFoundError:
                # If source is already gone, treat as done.
                if not os.path.exists(src):
                    return
                last_exc = None
                break
            except PermissionError as exc:
                last_exc = exc
                time.sleep(0.15 * (i + 1))
            except OSError as exc:
                last_exc = exc
                time.sleep(0.10 * (i + 1))
        if last_exc:
            raise RuntimeError(
                f'Could not rename frame file after {retries} retries: '
                f'{os.path.basename(src)} -> {os.path.basename(dst)} ({last_exc})'
            )

    def _model_is_installed(self, model_id: str) -> bool:
        if not config.UPSCAYL_MODELS_DIR:
            return False
        param = os.path.join(config.UPSCAYL_MODELS_DIR, f'{model_id}.param')
        binf  = os.path.join(config.UPSCAYL_MODELS_DIR, f'{model_id}.bin')
        return os.path.isfile(param) and os.path.isfile(binf)

    def _list_installed_models(self) -> list[str]:
        if not config.UPSCAYL_MODELS_DIR or not os.path.isdir(config.UPSCAYL_MODELS_DIR):
            return []
        out = []
        for fname in sorted(os.listdir(config.UPSCAYL_MODELS_DIR)):
            if not fname.endswith('.param'):
                continue
            mid = fname[:-6]
            if os.path.isfile(os.path.join(config.UPSCAYL_MODELS_DIR, f'{mid}.bin')):
                out.append(mid)
        return out

    def _log(self, log_path: str, msg: str):
        try:
            with open(log_path, 'a') as f:
                f.write(f'[{time.strftime("%H:%M:%S")}] {msg}\n')
        except Exception:
            pass

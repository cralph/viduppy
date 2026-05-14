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
                    update_job(job_id, {
                        'status': 'error',
                        'stage': f'Error: {exc}',
                        'error_msg': str(exc),
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
                self._upscale_frames(job_id, job, frames_dir, upscaled_dir, log_path)
                if self.queue.should_stop():
                    keep_workdirs = not self.queue.is_cancelled(job_id)
                    return
                source_dir = upscaled_dir
            else:
                out_w, out_h = self._desired_output_size(job)
                self._log(
                    log_path,
                    f'Skipping upscayl: downsize/same-size target ({out_w}x{out_h}) '
                    f'<= source ({job.get("width", 0)}x{job.get("height", 0)}).'
                )
                update_job(job_id, {
                    'stage': 'Redimensionando con FFmpeg (sin Upscayl)…',
                    'progress': 85,
                    'eta': 0,
                })
            self._assemble_video(
                job_id,
                job,
                source_dir,
                log_path,
                used_upscayl=use_upscayl,
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
                'stage': 'Reusando frames extraídos',
                'progress': 12,
                'frames_extracted': existing,
                'frames_to_process': existing,
            })
            return

        update_job(job_id, {
            'stage':    'Extrayendo frames',
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
                'stage':    f'Extrayendo frames ({count}/{total_exp})',
                'progress': round(pct * 0.12, 1),
                'frames_extracted': count,
            })
            time.sleep(0.5)

        proc.wait()
        if proc.returncode != 0:
            try:
                log_tail = open(ffmpeg_log).read()[-800:]
            except Exception:
                log_tail = '(sin log)'
            raise RuntimeError(f'FFmpeg extraction failed (code {proc.returncode}):\n{log_tail}')

        extracted = len(glob.glob(os.path.join(frames_dir, '*.png')))
        self._log(log_path, f'Extracted {extracted} frames OK')
        update_job(job_id, {
            'stage':    f'Frames extraídos: {extracted}',
            'progress': 12,
            'frames_extracted': extracted,
            'frames_to_process': extracted,
        })

    # ── Step 2 – Upscale frames ───────────────────────────────────────────────

    def _upscale_frames(self, job_id: str, job: dict,
                        frames_dir: str, upscaled_dir: str, log_path: str):

        all_frame_names = sorted(
            f for f in os.listdir(frames_dir)
            if f.endswith('.png') and not f.startswith('._')
        )
        total = len(all_frame_names)
        if total == 0:
            raise RuntimeError('No se extrajeron frames.')

        if not config.UPSCAYL_BIN:
            raise RuntimeError('upscayl-bin no encontrado. Configura la ruta en Configuración.')
        if not config.UPSCAYL_MODELS_DIR:
            raise RuntimeError('Directorio de modelos no encontrado. Configura la ruta en Configuración.')
        if not self._model_is_installed(job['model']):
            installed = ', '.join(self._list_installed_models()[:12]) or '(ninguno)'
            raise RuntimeError(
                f'El modelo "{job["model"]}" no está instalado en {config.UPSCAYL_MODELS_DIR}. '
                f'Modelos detectados: {installed}'
            )

        self._log(log_path, f'\n=== UPSCALE FRAMES ({total} frames) ===')
        self._log(log_path, f'binary={config.UPSCAYL_BIN}')
        self._log(log_path, f'models={config.UPSCAYL_MODELS_DIR}')
        self._log(log_path, f'scale={job["scale"]}  model={job["model"]}')

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

        if not needed:
            self._log(log_path, 'All frames already upscaled, skipping upscayl-bin.')
        else:
            # ── Build a subset dir with hard links (or copies) ────────────────
            # upscayl-bin works reliably with DIRECTORY input, not single files.
            # This matches the confirmed-working reference implementation.
            subset_dir = os.path.join(
                os.path.dirname(frames_dir), f'subset_{job_id[:8]}'
            )
            shutil.rmtree(subset_dir, ignore_errors=True)
            os.makedirs(subset_dir)

            try:
                for fname in needed:
                    src = os.path.join(frames_dir, fname)
                    dst = os.path.join(subset_dir, fname)
                    try:
                        os.link(src, dst)       # fast hard link (no extra disk)
                    except OSError:
                        shutil.copy2(src, dst)  # fallback: copy

                # ── Resolve models path relative to binary dir ────────────────
                # Some builds of upscayl-bin prepend their own directory to the
                # -m argument, so an absolute path like /foo/models becomes
                # /foo/bin//foo/models (broken). Passing a relative path fixes it.
                bin_dir = os.path.dirname(os.path.abspath(config.UPSCAYL_BIN))
                try:
                    models_arg = os.path.relpath(config.UPSCAYL_MODELS_DIR, bin_dir)
                except ValueError:
                    # Windows: relpath fails across drives — fall back to absolute
                    models_arg = config.UPSCAYL_MODELS_DIR
                self._log(log_path,
                    f'bin_dir={bin_dir}  models_abs={config.UPSCAYL_MODELS_DIR}  '
                    f'models_rel={models_arg}')

                # ── Run upscayl-bin on the whole directory ────────────────────
                # NOTE: scale flag is -z (not -s) — confirmed from working reference.
                cmd = [
                    config.UPSCAYL_BIN,
                    '-i', subset_dir,
                    '-o', upscaled_dir,
                    '-m', models_arg,
                    '-n', job['model'],
                    '-z', str(job['scale']),
                    '-f', 'png',
                ]
                gpu_flags = self._upscayl_gpu_flags(log_path)
                if gpu_flags:
                    cmd += gpu_flags
                self._log(log_path, 'CMD: ' + ' '.join(cmd))
                update_job(job_id, {
                    'stage':    f'Upscaleando {len(needed)} frames…',
                    'progress': 12,
                    'frames_to_process': total,
                })

                def _run_upscayl(extra_flags: list, label: str):
                    full_cmd = cmd + extra_flags
                    self._log(log_path, f'CMD [{label}]: ' + ' '.join(full_cmd))
                    upscayl_log = os.path.join(
                        config.OUTPUT_FOLDER, f'{job_id}_upscayl_{label}.log'
                    )
                    log_f = open(upscayl_log, 'a', encoding='utf-8', errors='replace')
                    p = subprocess.Popen(
                        full_cmd,
                        stdout=log_f,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )
                    # ETA based on real frames processed:
                    # eta = (elapsed / processed) * remaining
                    _start_t = time.time()
                    _start_done = min(total, self._count_pngs(upscaled_dir))
                    _last_done = -1

                    def _update_from_counts(force: bool = False):
                        nonlocal _last_done
                        done = min(total, self._count_pngs(upscaled_dir))
                        pct  = (done / max(total, 1)) * 100
                        now  = time.time()

                        if force or done != _last_done:
                            _last_done = done
                            overall = 12 + pct * 0.73
                            processed = max(0, done - _start_done)
                            remaining = max(0, total - done)
                            elapsed = max(0.001, now - _start_t)
                            eta = int((elapsed / processed) * remaining) if processed > 0 else 0
                            update_job(job_id, {
                                'stage':         f'Upscaleando frames ({done}/{total}) · {pct:.1f}%',
                                'progress':      round(overall, 1),
                                'current_frame': done,
                                'frames_to_process': total,
                                'eta':           eta,
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
                                return p
                            time.sleep(0.5)
                            _update_from_counts()
                    finally:
                        log_f.close()

                    _update_from_counts(force=True)
                    p.wait()
                    return p

                # ── Run upscayl-bin on GPU ────────────────────────────────────────
                # NOTE: this build of upscayl-bin does NOT support -g -1 (CPU mode).
                # FORCE_CPU is kept in settings for future compatibility but is a no-op here.
                if config.FORCE_CPU:
                    self._log(log_path,
                        'FORCE_CPU=True pero esta versión de upscayl-bin no soporta '
                        '-g -1 (CPU). Ejecutando en GPU de todas formas.')

                proc = _run_upscayl([], 'gpu')
                if self.queue.should_stop():
                    return
                if proc.returncode != 0:
                    raise RuntimeError(
                        f'Upscayl-bin falló (code {proc.returncode}). '
                        f'Verifica que el modelo "{job["model"]}" está instalado '
                        f'(necesita {job["model"]}.param y {job["model"]}.bin en el '
                        f'directorio de modelos). Revisa log: {log_path}'
                    )

                # Check if GPU silently produced black frames (model not installed)
                sample_pngs = sorted(
                    f for f in os.listdir(upscaled_dir)
                    if f.endswith('.png') and not f.startswith('._')
                )
                if sample_pngs and self._is_black_frame(
                        os.path.join(upscaled_dir, sample_pngs[0])):
                    self._log(log_path,
                        '⚠ GPU produjo frames negros. Causa más probable: el modelo '
                        f'"{job["model"]}" no está instalado en el directorio de modelos. '
                        'Verifica que existan los archivos .param y .bin correspondientes.')
                    raise RuntimeError(
                        f'Los frames upscaleados están negros. '
                        f'El modelo "{job["model"]}" probablemente no está instalado: '
                        f'necesita {job["model"]}.param y {job["model"]}.bin. '
                        f'Selecciona un modelo instalado en Configuración y reintenta.'
                    )

            finally:
                shutil.rmtree(subset_dir, ignore_errors=True)

        # ── Strip upscayl suffixes from output filenames ──────────────────────
        with os.scandir(upscaled_dir) as scan:
            for entry in scan:
                if not (entry.is_file() and entry.name.endswith('.png')):
                    continue
                if '_upscayl' in entry.name or '_out' in entry.name:
                    m = _re.match(r'(frame_\d+)', entry.name)
                    if m:
                        new_path = os.path.join(upscaled_dir, m.group(1) + '.png')
                        if not os.path.exists(new_path):
                            os.replace(entry.path, new_path)
                        else:
                            os.remove(entry.path)

        # ── Verify frame count ────────────────────────────────────────────────
        final_pngs = sorted(
            f for f in os.listdir(upscaled_dir)
            if f.endswith('.png') and not f.startswith('._')
        )
        self._log(log_path, f'After upscaling: {len(final_pngs)} PNGs in upscaled_dir')

        if len(final_pngs) < total:
            raise RuntimeError(
                f'Faltan frames upscaleados: solo {len(final_pngs)} de {total}. '
                f'Revisa log: {log_path}'
            )

        # ── Black-frame sanity check ──────────────────────────────────────────
        first_png = os.path.join(upscaled_dir, final_pngs[0])
        if self._is_black_frame(first_png):
            self._log(log_path,
                f'WARNING: El primer frame upscaleado es completamente negro. '
                f'El modelo "{job["model"]}" puede no estar instalado '
                f'(faltan {job["model"]}.param o {job["model"]}.bin). '
                f'Selecciona un modelo instalado en Configuración.'
            )
            raise RuntimeError(
                f'Los frames upscaleados están negros. '
                f'El modelo "{job["model"]}" probablemente no está instalado. '
                f'Verifica que existen {job["model"]}.param y {job["model"]}.bin '
                f'en el directorio de modelos y selecciona un modelo válido.'
            )

        # ── Normalize to frame_000001.png … for FFmpeg ────────────────────────
        for idx, fname in enumerate(final_pngs):
            src    = os.path.join(upscaled_dir, fname)
            target = os.path.join(upscaled_dir, f'frame_{idx + 1:06d}.png')
            if os.path.normcase(src) != os.path.normcase(target):
                os.replace(src, target)

        self._log(log_path, f'Upscaling done. {total} frames ready.')
        update_job(job_id, {'stage': 'Frames upscaleados, armando video…', 'progress': 85, 'eta': 0})

    # ── Step 3 – Reassemble video ─────────────────────────────────────────────

    def _assemble_video(self, job_id: str, job: dict, frames_input_dir: str,
                        log_path: str, used_upscayl: bool,
                        process_started_at: float, base_elapsed: float):
        update_job(job_id, {'stage': 'Armando video final', 'progress': 87})
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
            raise RuntimeError('No hay frames disponibles para armar el video.')

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

        scale_filter = self._build_output_scale_filter(job, used_upscayl)
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
                'stage':    'Armando video final · codificando video',
                'progress': 90,
                'eta':       0,
            })
            self._run_log(encode_base + [tmp_path], log_path)

            update_job(job_id, {
                'stage':    'Armando video final · multiplexando audio',
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
                'stage':    'Armando video final · codificando video',
                'progress': 90,
                'eta':       0,
            })
            self._run_log(encode_base + [out_path], log_path)

        self._log(log_path, f'Assembly complete: {out_path}')
        total_elapsed = base_elapsed + max(0.0, time.time() - process_started_at)
        update_job(job_id, {
            'status':       'completed',
            'stage':        'Completado ✓',
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

    def _calc_eta(self, frames_remaining: int) -> int:
        if not self._recent_frame_times:
            return 0
        avg = sum(self._recent_frame_times) / len(self._recent_frame_times)
        return int(avg * frames_remaining)

    def _count_pngs(self, directory: str) -> int:
        """Count PNG files with shell-equivalent semantics: ls <dir>/*.png | wc -l."""
        return len(glob.glob(os.path.join(directory, '*.png')))

    def _build_output_scale_filter(self, job: dict, used_upscayl: bool) -> str:
        """Build optional FFmpeg scale filter for final output sizing."""
        out_w, out_h = self._desired_output_size(job)
        in_w = int(job.get('width', 0) or 0)
        in_h = int(job.get('height', 0) or 0)
        if used_upscayl:
            scale = max(1, int(job.get('scale', 1) or 1))
            in_w *= scale
            in_h *= scale

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

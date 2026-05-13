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

        update_job(job_id, {'status': 'processing', 'started_at': time.time()})

        try:
            self._extract_frames(job_id, job, frames_dir, log_path)
            if self.queue.should_stop():
                return
            self._upscale_frames(job_id, job, frames_dir, upscaled_dir, log_path)
            if self.queue.should_stop():
                return
            self._assemble_video(job_id, job, upscaled_dir, log_path)
        finally:
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

        update_job(job_id, {
            'stage':    'Extrayendo frames',
            'progress': 0,
            'frames_to_process': total_exp,
        })

        self._log(log_path, f'=== EXTRACT FRAMES ===')
        self._log(log_path, f'start_time={start_time:.4f}s  duration={duration:.4f}s  expected={total_exp}')

        ffmpeg_log = os.path.join(frames_dir, '_ffmpeg_extract.log')
        cmd = [
            'ffmpeg', '-y',
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
                self._log(log_path, 'CMD: ' + ' '.join(cmd))
                update_job(job_id, {
                    'stage':    f'Upscaleando {len(needed)} frames…',
                    'progress': 12,
                    'frames_to_process': total,
                })

                def _run_upscayl(extra_flags: list, label: str):
                    full_cmd = cmd + extra_flags
                    self._log(log_path, f'CMD [{label}]: ' + ' '.join(full_cmd))
                    p = subprocess.Popen(
                        full_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        bufsize=1,
                    )
                    # EMA (exponential moving average) for smooth ETA.
                    # We track seconds-per-percent-point and smooth with alpha=0.2.
                    _ema_spp  = None   # seconds per % point, EMA
                    _last_pct = 0.0
                    _last_t   = time.time()
                    EMA_ALPHA = 0.2

                    for line in p.stdout:
                        if self.queue.should_stop():
                            p.terminate()
                            return p
                        line = line.rstrip()
                        if line:
                            self._log(log_path, f'  [upscayl/{label}] {line}')
                        pct_m = _re.search(r'(\d+(?:\.\d+)?)\s*%', line)
                        if pct_m:
                            pct = float(pct_m.group(1))
                            now = time.time()

                            # Update EMA speed estimate
                            dpct = pct - _last_pct
                            dt   = now - _last_t
                            if dpct > 0 and dt > 0:
                                spp = dt / dpct   # seconds per % point
                                _ema_spp = (spp if _ema_spp is None
                                            else EMA_ALPHA * spp + (1 - EMA_ALPHA) * _ema_spp)
                            _last_pct = pct
                            _last_t   = now

                            overall = 12 + pct * 0.73
                            eta = int(_ema_spp * (100 - pct)) if _ema_spp and pct < 100 else 0
                            update_job(job_id, {
                                'stage':         f'Upscaleando frames… {pct:.1f}%',
                                'progress':      round(overall, 1),
                                'current_frame': int(len(needed) * pct / 100),
                                'eta':           eta,
                            })
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

    def _assemble_video(self, job_id: str, job: dict, upscaled_dir: str, log_path: str):
        update_job(job_id, {'stage': 'Armando video final', 'progress': 87})
        self._log(log_path, '\n=== ASSEMBLE VIDEO ===')

        safe_model = job['model'].replace('/', '_')
        out_name   = f"upscaled_{job['id'][:8]}_{job['scale']}x_{safe_model}.mp4"
        out_path   = os.path.join(config.OUTPUT_FOLDER, out_name)
        fps        = job['fps']
        has_audio  = self._has_audio(job['filepath'])

        frame_pattern = os.path.join(upscaled_dir, 'frame_%06d.png')

        # Verify we actually have frames before trying to encode
        png_count = len(glob.glob(os.path.join(upscaled_dir, '*.png')))
        self._log(log_path, f'fps={fps}  has_audio={has_audio}  png_count={png_count}  out={out_path}')
        if png_count == 0:
            raise RuntimeError('No hay frames upscaleados en el directorio de salida.')

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

        encode_base = [
            'ffmpeg', '-y',
            '-start_number', '1',      # explicit: frames are frame_000001.png…
            '-framerate', str(fps),
            '-i', frame_pattern,
        ] + encoder_args

        if has_audio:
            start_t  = job['start_frame'] / fps
            duration = (job['end_frame'] - job['start_frame']) / fps
            tmp_path = out_path.replace('.mp4', '_novid.mp4')

            self._run_log(encode_base + [tmp_path], log_path)
            self._run_log([
                'ffmpeg', '-y',
                '-i', tmp_path,
                '-ss', f'{start_t:.4f}', '-t', f'{duration:.4f}',
                '-i', job['filepath'],
                '-c:v', 'copy', '-c:a', 'aac',
                '-map', '0:v:0', '-map', '1:a:0',
                '-shortest', out_path,
            ], log_path)
            os.remove(tmp_path)
        else:
            self._run_log(encode_base + [out_path], log_path)

        self._log(log_path, f'Assembly complete: {out_path}')
        update_job(job_id, {
            'status':       'completed',
            'stage':        'Completado ✓',
            'progress':     100,
            'output_path':  out_path,
            'completed_at': time.time(),
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

    def _is_black_frame(self, png_path: str) -> bool:
        """Downscale PNG to 1×1 via ffmpeg and check if average colour is near black."""
        try:
            r = subprocess.run(
                ['ffmpeg', '-loglevel', 'error',
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
        try:
            r = subprocess.run(
                ['ffmpeg', '-loglevel', 'error',
                 '-f', 'lavfi', '-i', 'nullsrc=s=64x64:d=0.1',
                 '-c:v', 'h264_nvenc', '-f', 'null', '-'],
                capture_output=True, timeout=10,
            )
            return r.returncode == 0
        except Exception:
            return False

    def _has_audio(self, filepath: str) -> bool:
        r = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-show_streams',
             '-select_streams', 'a', filepath],
            capture_output=True, text=True,
        )
        return 'codec_type=audio' in r.stdout

    def _calc_eta(self, frames_remaining: int) -> int:
        if not self._recent_frame_times:
            return 0
        avg = sum(self._recent_frame_times) / len(self._recent_frame_times)
        return int(avg * frames_remaining)

    def _log(self, log_path: str, msg: str):
        try:
            with open(log_path, 'a') as f:
                f.write(f'[{time.strftime("%H:%M:%S")}] {msg}\n')
        except Exception:
            pass

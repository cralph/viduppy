import json
import os
import platform
import re
import subprocess
import threading
import time
import uuid

from flask import (Flask, Response, jsonify, redirect, render_template,
                   request, send_file, url_for)

import config
from database import (create_job, delete_job_record, get_all_jobs, get_job,
                      init_db, update_job)
from processor import VideoProcessor
from queue_manager import QueueManager

# ── App setup ─────────────────────────────────────────────────────────────────

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 20 * 1024 * 1024 * 1024  # 20 GB

queue   = QueueManager()
proc    = VideoProcessor(queue)
worker  = threading.Thread(target=proc.run, daemon=True)
_worker_lock = threading.Lock()
_runtime_lock = threading.Lock()
_runtime_ready = False
FRAME_PREVIEW_DIR = os.path.join(config.OUTPUT_FOLDER, 'frame_previews')

# ── Helpers ───────────────────────────────────────────────────────────────────

def _probe(filepath: str) -> dict:
    """Run ffprobe and return parsed JSON."""
    ffprobe_exec = config.FFPROBE_BIN or 'ffprobe'
    r = subprocess.run(
        [ffprobe_exec, '-v', 'quiet', '-print_format', 'json',
         '-show_streams', '-show_format', filepath],
        capture_output=True, text=True,
    )
    return json.loads(r.stdout) if r.returncode == 0 else {}


def _video_info(filepath: str) -> dict:
    data   = _probe(filepath)
    vstrm  = next((s for s in data.get('streams', [])
                   if s.get('codec_type') == 'video'), {})
    fmt    = data.get('format', {})

    # fps can be expressed as fraction string "30000/1001"
    fps_raw = vstrm.get('r_frame_rate', '30/1')
    try:
        num, den = fps_raw.split('/')
        fps = round(int(num) / int(den), 3)
    except Exception:
        fps = 30.0

    duration     = float(fmt.get('duration', 0))
    nb_frames    = vstrm.get('nb_frames')
    total_frames = int(nb_frames) if nb_frames else int(duration * fps)

    return {
        'fps':          fps,
        'total_frames': total_frames,
        'duration':     duration,
        'width':        int(vstrm.get('width', 0)),
        'height':       int(vstrm.get('height', 0)),
    }


def _run_cmd_capture(cmd: list[str]) -> tuple[int, str]:
    r = subprocess.run(cmd, capture_output=True, text=True)
    out = (r.stdout or '') + '\n' + (r.stderr or '')
    return r.returncode, out.strip()


def _cleanup_preview_cache(max_files: int = 120):
    try:
        if not os.path.isdir(FRAME_PREVIEW_DIR):
            return
        files = []
        for name in os.listdir(FRAME_PREVIEW_DIR):
            path = os.path.join(FRAME_PREVIEW_DIR, name)
            if os.path.isfile(path):
                files.append(path)
        if len(files) <= max_files:
            return
        files.sort(key=lambda p: os.path.getmtime(p))
        for p in files[:-max_files]:
            try:
                os.remove(p)
            except OSError:
                pass
    except Exception:
        pass


def _is_safe_uploaded_path(filepath: str) -> bool:
    try:
        file_abs = os.path.normcase(os.path.abspath(filepath))
        uploads_abs = os.path.normcase(os.path.abspath(config.UPLOAD_FOLDER))
        return file_abs.startswith(uploads_abs + os.sep)
    except Exception:
        return False


def ensure_worker_running() -> bool:
    """
    Ensure the background queue worker thread is alive.
    Returns True when a thread was started/restarted in this call.
    """
    global worker
    with _worker_lock:
        if worker.is_alive():
            return False
        if worker.ident is not None:
            # A Python thread object cannot be started twice.
            worker = threading.Thread(target=proc.run, daemon=True)
        worker.start()
        return True


def ensure_runtime_ready():
    """Ensure DB schema exists and worker thread is running."""
    global _runtime_ready
    with _runtime_lock:
        if not _runtime_ready:
            init_db()
            _runtime_ready = True
    ensure_worker_running()


# ── Pages ─────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('app.html')


@app.before_request
def _ensure_runtime_for_requests():
    ensure_runtime_ready()


# Legacy redirects (keep old URLs working)
@app.route('/new')
def new_job():
    return redirect('/')

@app.route('/job/<job_id>')
def job_detail(job_id):
    return redirect('/')


# ── Models API ────────────────────────────────────────────────────────────────

MODEL_DESCS = {
    'realesrgan-x4plus':       'Best for real-world photos & videos',
    'realesrgan-x4plus-anime': 'Optimized for anime & cartoon',
    'realesrnet-x4plus':       'Faster processing, good quality',
    'ultrasharp':              'Ultra detail enhancement',
    'digital-art':             'For digital artwork & illustrations',
    'remacri':                 'Balanced sharpness & clarity',
    'ultramix-balanced':       'Mixed enhancement blend',
    'high-fidelity-4x':        'Maximum fidelity upscaling',
}

@app.route('/api/models')
def api_models():
    """Return only models whose .param file actually exists in the models dir."""
    models_dir = config.UPSCAYL_MODELS_DIR

    installed = []
    if models_dir and os.path.isdir(models_dir):
        # Scan .param files — each model needs modelname.param + modelname.bin
        for fname in sorted(os.listdir(models_dir)):
            if not fname.endswith('.param'):
                continue
            model_id = fname[:-6]          # strip ".param"
            bin_file = os.path.join(models_dir, model_id + '.bin')
            if not os.path.isfile(bin_file):
                continue                   # .param without .bin — skip
            installed.append({
                'id':   model_id,
                'name': model_id.replace('-', ' ').title(),
                'desc': MODEL_DESCS.get(model_id, ''),
            })

    # Fall back to full hardcoded list if dir not accessible yet (settings not saved)
    if not installed:
        installed = [
            {**m, 'desc': MODEL_DESCS.get(m['id'], '')}
            for m in config.UPSCAYL_MODELS
        ]

    return jsonify({'models': installed, 'models_dir': models_dir or ''})


# ── GPU info ──────────────────────────────────────────────────────────────────

@app.route('/api/gpu')
def api_gpu():
    """Detect NVENC support and list Vulkan/NVIDIA GPUs."""
    import re

    ffmpeg_exec = config.FFMPEG_BIN or 'ffmpeg'
    nvenc_ok = False
    try:
        r = subprocess.run(
            [ffmpeg_exec, '-loglevel', 'error',
             '-f', 'lavfi', '-i', 'nullsrc=s=256x256:d=0.1',
             '-c:v', 'h264_nvenc', '-f', 'null', '-'],
            capture_output=True, timeout=10,
        )
        nvenc_ok = (r.returncode == 0)
    except Exception:
        pass

    gpus = []
    # Prefer nvidia-smi (cleanest GPU names)
    try:
        r2 = subprocess.run(
            ['nvidia-smi', '--query-gpu=index,name', '--format=csv,noheader'],
            capture_output=True, text=True, timeout=5,
        )
        if r2.returncode == 0:
            for line in r2.stdout.splitlines():
                parts = line.strip().split(',', 1)
                if len(parts) == 2:
                    gpus.append({'id': int(parts[0].strip()), 'name': parts[1].strip()})
    except Exception:
        pass

    # Windows fallback: list display adapters even when nvidia-smi is not on PATH.
    if not gpus and platform.system() == 'Windows':
        try:
            r2b = subprocess.run(
                ['powershell', '-NoProfile', '-Command',
                 'Get-CimInstance Win32_VideoController | '
                 'Select-Object -ExpandProperty Name'],
                capture_output=True, text=True, timeout=8,
            )
            if r2b.returncode == 0:
                for idx, line in enumerate(r2b.stdout.splitlines()):
                    name = line.strip()
                    if name:
                        gpus.append({'id': idx, 'name': name})
        except Exception:
            pass

    # Fallback: try ffmpeg Vulkan device enumeration
    if not gpus:
        try:
            r3 = subprocess.run(
                [ffmpeg_exec, '-loglevel', 'verbose',
                 '-init_hw_device', 'vulkan=vk:0', '-f', 'null', '-'],
                capture_output=True, text=True, timeout=8,
            )
            for line in (r3.stdout + r3.stderr).splitlines():
                m = re.search(r'(?:device|Device)\s*(\d+).*?:\s*(.+)', line)
                if m:
                    name = m.group(2).strip()
                    idx  = int(m.group(1))
                    if name and not any(g['id'] == idx for g in gpus):
                        gpus.append({'id': idx, 'name': name})
        except Exception:
            pass

    return jsonify({
        'nvenc_available': nvenc_ok,
        'gpus':            gpus,
        'ffmpeg_path':      ffmpeg_exec,
        'use_nvenc':       config.USE_NVENC,
        'gpu_device':      config.GPU_DEVICE,
        'force_cpu':       config.FORCE_CPU,
        'windows_safe_mode': config.WINDOWS_SAFE_MODE,
    })


# ── Upload & create ───────────────────────────────────────────────────────────

@app.route('/upload', methods=['POST'])
def upload():
    if 'video' not in request.files:
        return jsonify({'error': 'No file was uploaded'}), 400

    f   = request.files['video']
    ext = (f.filename or '').rsplit('.', 1)[-1].lower()
    if ext not in config.ALLOWED_EXTENSIONS:
        return jsonify({'error': f'Unsupported .{ext} format'}), 400

    job_id   = str(uuid.uuid4())
    filename = f'{job_id}.{ext}'
    filepath = os.path.join(config.UPLOAD_FOLDER, filename)
    f.save(filepath)

    info = _video_info(filepath)
    return jsonify({
        'job_id':   job_id,
        'filename': f.filename,
        'filepath': filepath,
        **info,
    })


@app.route('/job/create', methods=['POST'])
def create_job_route():
    d = request.json or {}
    required = ['job_id', 'filename', 'filepath', 'scale', 'model',
                'start_frame', 'end_frame', 'total_frames', 'fps',
                'duration', 'width', 'height']
    for k in required:
        if k not in d:
            return jsonify({'error': f'Missing field: {k}'}), 400

    output_factor = float(d.get('output_factor', 1.0) or 1.0)
    target_width  = int(d.get('target_width', 0) or 0)
    target_height = int(d.get('target_height', 0) or 0)
    if output_factor <= 0:
        return jsonify({'error': 'output_factor must be > 0'}), 400
    if target_width < 0 or target_height < 0:
        return jsonify({'error': 'target_width/target_height cannot be negative'}), 400

    create_job({
        'id':            d['job_id'],
        'original_name': d['filename'],
        'filepath':      d['filepath'],
        'scale':         int(d['scale']),
        'model':         d['model'],
        'start_frame':   int(d['start_frame']),
        'end_frame':     int(d['end_frame']),
        'total_frames':  int(d['total_frames']),
        'fps':           float(d['fps']),
        'duration':      float(d['duration']),
        'width':         int(d['width']),
        'height':        int(d['height']),
        'output_factor': output_factor,
        'target_width':  target_width,
        'target_height': target_height,
        'status':        'queued',
        'stage':         'Queued',
        'progress':      0,
        'created_at':    time.time(),
    })
    queue.add_job(d['job_id'])
    return jsonify({'success': True, 'job_id': d['job_id']})


# ── Job controls ──────────────────────────────────────────────────────────────

@app.route('/job/<job_id>/pause', methods=['POST'])
def pause_job(job_id):
    job = get_job(job_id)
    prev_stage = (job or {}).get('stage') or 'Processing'
    # Accumulate elapsed time so the timer survives pause/resume cycles
    prev_elapsed = float((job or {}).get('elapsed_time', 0) or 0)
    started_at   = float((job or {}).get('started_at',   0) or 0)
    elapsed_now  = prev_elapsed + (time.time() - started_at if started_at else 0)
    queue.pause_job(job_id)
    update_job(job_id, {
        'status':       'paused',
        'stage':        f'Paused at {prev_stage}',
        'elapsed_time': round(elapsed_now, 1),
    })
    return jsonify({'ok': True})


@app.route('/job/<job_id>/resume', methods=['POST'])
def resume_job(job_id):
    resumed = queue.resume_job(job_id)
    if not resumed:
        # Safety net: after a server restart the in-memory paused set is empty.
        # Ensure the job is still re-enqueued when user clicks Resume.
        queue.add_job(job_id)
    # Reset started_at so the frontend delta is correct from this moment
    update_job(job_id, {
        'status':     'queued',
        'stage':      'Queued (resumed)',
        'started_at': time.time(),
    })
    return jsonify({'ok': True})


@app.route('/job/<job_id>/cancel', methods=['POST'])
def cancel_job(job_id):
    job = get_job(job_id)
    prev_stage = (job or {}).get('stage') or 'Processing'
    prev_elapsed = float((job or {}).get('elapsed_time', 0) or 0)
    started_at   = float((job or {}).get('started_at',   0) or 0)
    elapsed_now  = prev_elapsed + (time.time() - started_at if started_at else 0)
    queue.cancel_job(job_id)
    update_job(job_id, {
        'status':       'cancelled',
        'stage':        f'Cancelled at {prev_stage}',
        'elapsed_time': round(elapsed_now, 1),
        'completed_at': time.time(),
        'eta':          0,
    })
    return jsonify({'ok': True})


@app.route('/job/<job_id>/reprocess', methods=['POST'])
def reprocess_job(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({'error': 'Not found'}), 404
    update_job(job_id, {
        'status': 'queued', 'stage': 'Queued (reprocess)',
        'progress': 0, 'current_frame': 0, 'frames_extracted': 0,
        'output_path': None, 'error_msg': None,
        'started_at': None, 'completed_at': None, 'eta': None,
        'elapsed_time': 0,   # reset accumulated time on full reprocess
    })
    queue.add_job(job_id)
    return jsonify({'ok': True})


@app.route('/job/<job_id>/delete', methods=['POST'])
def delete_job(job_id):
    queue.cancel_job(job_id)
    job = get_job(job_id)
    if job:
        # Remove uploaded file
        if job.get('filepath') and os.path.isfile(job['filepath']):
            os.remove(job['filepath'])
        delete_job_record(job_id)
    return jsonify({'ok': True})


@app.route('/job/<job_id>/priority', methods=['POST'])
def change_priority(job_id):
    direction = (request.json or {}).get('direction', 'up')
    ok = queue.change_priority(job_id, direction)
    return jsonify({'ok': ok})


# ── Job log ───────────────────────────────────────────────────────────────────

@app.route('/api/job/<job_id>/log')
def job_log(job_id):
    log_path = os.path.join(config.OUTPUT_FOLDER, f'{job_id}.log')
    if not os.path.isfile(log_path):
        return jsonify({'log': '(no log file found)'}), 404
    try:
        content = open(log_path, encoding='utf-8', errors='replace').read()
    except Exception as e:
        content = f'Error reading log: {e}'
    return jsonify({'log': content})


# ── Status & streaming ────────────────────────────────────────────────────────

@app.route('/api/jobs')
def api_jobs():
    return jsonify({
        'jobs':   get_all_jobs(),
        'queue':  queue.get_queue(),
        'active': queue.active_job,
    })


@app.route('/api/job/<job_id>')
def api_job(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({'error': 'not found'}), 404
    return jsonify(job)


@app.route('/events')
def sse():
    """Server-Sent Events stream for real-time updates."""
    def stream():
        prev = ''
        while True:
            payload = json.dumps({
                'jobs':   get_all_jobs(),
                'queue':  queue.get_queue(),
                'active': queue.active_job,
            })
            if payload != prev:
                prev = payload
                yield f'data: {payload}\n\n'
            time.sleep(1)

    return Response(stream(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache',
                             'X-Accel-Buffering': 'no'})


# ── File serving ──────────────────────────────────────────────────────────────

@app.route('/video/<job_id>')
def serve_video(job_id):
    # Serve directly by filename — no DB needed (preview happens before job creation)
    for ext in config.ALLOWED_EXTENSIONS:
        path = os.path.join(config.UPLOAD_FOLDER, f'{job_id}.{ext}')
        if os.path.isfile(path):
            return send_file(path, conditional=True)
    # Fallback: DB lookup (for already-created jobs)
    job = get_job(job_id)
    if job and os.path.isfile(job.get('filepath', '')):
        return send_file(job['filepath'], conditional=True)
    return 'Not found', 404


@app.route('/output/<job_id>')
def download_output(job_id):
    job = get_job(job_id)
    if not job or not job.get('output_path'):
        return 'Not found', 404
    name = f"upscaled_{job['original_name'].rsplit('.', 1)[0]}_{job['scale']}x.mp4"
    return send_file(job['output_path'], as_attachment=True, download_name=name)


@app.route('/output/preview/<job_id>')
def preview_output(job_id):
    job = get_job(job_id)
    if not job or not job.get('output_path'):
        return 'Not found', 404
    return send_file(job['output_path'])


@app.route('/api/preview/frame', methods=['POST'])
def preview_frame():
    d = request.json or {}
    filepath = config._normalize_path(d.get('filepath', ''))
    if not filepath or not os.path.isfile(filepath):
        return jsonify({'error': 'Video file not found for preview.'}), 404
    if not _is_safe_uploaded_path(filepath):
        return jsonify({'error': 'Invalid preview file path.'}), 400

    model = str(d.get('model', '') or '').strip()
    if not model:
        return jsonify({'error': 'Missing model for preview.'}), 400
    if not config.UPSCAYL_BIN or not os.path.isfile(config.UPSCAYL_BIN):
        return jsonify({'error': 'Upscayl binary not configured.'}), 400
    if not config.UPSCAYL_MODELS_DIR or not os.path.isdir(config.UPSCAYL_MODELS_DIR):
        return jsonify({'error': 'Upscayl models directory not configured.'}), 400
    if not proc._model_is_installed(model):
        installed = ', '.join(proc._list_installed_models()[:12]) or '(none)'
        return jsonify({
            'error': f'Model "{model}" is not installed. Installed models: {installed}'
        }), 400

    try:
        scale = max(1, int(d.get('scale', 2)))
        duration = max(0.0, float(d.get('duration', 0) or 0))
        time_sec = max(0.0, float(d.get('time_sec', 0) or 0))
        if duration > 0:
            time_sec = min(time_sec, duration)
        width = max(0, int(d.get('width', 0) or 0))
        height = max(0, int(d.get('height', 0) or 0))
        output_factor = float(d.get('output_factor', 1.0) or 1.0)
        if output_factor <= 0:
            output_factor = 1.0
        target_width = max(0, int(d.get('target_width', 0) or 0))
        target_height = max(0, int(d.get('target_height', 0) or 0))
    except Exception:
        return jsonify({'error': 'Invalid preview parameters.'}), 400

    job_like = {
        'scale': scale,
        'model': model,
        'width': width,
        'height': height,
        'output_factor': output_factor,
        'target_width': target_width,
        'target_height': target_height,
    }
    upscayl_factor = proc._effective_upscayl_factor(job_like)
    target_w, target_h = proc._desired_output_size(job_like)

    os.makedirs(FRAME_PREVIEW_DIR, exist_ok=True)
    token = uuid.uuid4().hex[:12]
    orig_path = os.path.join(FRAME_PREVIEW_DIR, f'{token}_orig.png')
    up_raw_path = os.path.join(FRAME_PREVIEW_DIR, f'{token}_up_raw.png')
    up_path = os.path.join(FRAME_PREVIEW_DIR, f'{token}_up.png')
    log_path = os.path.join(FRAME_PREVIEW_DIR, f'{token}.log')

    ffmpeg_exec = config.FFMPEG_BIN or 'ffmpeg'
    extract_cmd = [
        ffmpeg_exec, '-y',
        '-ss', f'{time_sec:.4f}',
        '-i', filepath,
        '-frames:v', '1',
        orig_path,
    ]
    rc, out = _run_cmd_capture(extract_cmd)
    if rc != 0 or not os.path.isfile(orig_path):
        return jsonify({
            'error': f'Could not extract frame at {time_sec:.3f}s.',
            'details': out[-500:],
        }), 500

    scale_args = proc._upscayl_scale_args(upscayl_factor, log_path)
    gpu_flags = proc._upscayl_gpu_flags(log_path)
    cmd_base = [
        config.UPSCAYL_BIN,
        '-i', orig_path,
        '-o', up_raw_path,
        '-m', config.UPSCAYL_MODELS_DIR,
        '-n', model,
        '-f', 'png',
    ] + scale_args
    cmd_preferred = cmd_base + (gpu_flags or [])

    def _run_up_cmd(cmd: list[str]) -> tuple[int, str]:
        rc_local, txt = _run_cmd_capture(cmd)
        try:
            with open(log_path, 'a', encoding='utf-8', errors='replace') as f:
                f.write('CMD: ' + ' '.join(cmd) + '\n')
                f.write(f'RC: {rc_local}\n')
                if txt:
                    f.write(txt[-1200:] + '\n')
        except Exception:
            pass
        return rc_local, txt

    rc, out = _run_up_cmd(cmd_preferred)
    if rc in (3221225477, -1073741819) and gpu_flags:
        rc, out = _run_up_cmd(cmd_base)
    if rc != 0:
        return jsonify({
            'error': f'Upscayl preview failed (code {rc}).',
            'details': out[-500:],
        }), 500

    if not os.path.isfile(up_raw_path):
        prefix = os.path.splitext(os.path.basename(orig_path))[0]
        cands = sorted(
            p for p in os.listdir(FRAME_PREVIEW_DIR)
            if p.startswith(prefix) and p.endswith('.png') and p != os.path.basename(orig_path)
        )
        if cands:
            best = max(
                (os.path.join(FRAME_PREVIEW_DIR, c) for c in cands),
                key=lambda p: os.path.getsize(p),
            )
            try:
                os.replace(best, up_raw_path)
            except OSError:
                pass

    if not os.path.isfile(up_raw_path):
        return jsonify({'error': 'Upscayl preview output was not created.'}), 500

    # Match expected final dimensions if current upscaled frame differs.
    probe_w, probe_h = proc._probe_image_size(up_raw_path)
    if target_w > 0 and target_h > 0 and (probe_w != target_w or probe_h != target_h):
        resize_cmd = [
            ffmpeg_exec, '-y',
            '-i', up_raw_path,
            '-vf', f'scale={target_w}:{target_h}',
            up_path,
        ]
        rc, out = _run_cmd_capture(resize_cmd)
        if rc != 0 or not os.path.isfile(up_path):
            return jsonify({
                'error': 'Could not resize preview frame to final dimensions.',
                'details': out[-500:],
            }), 500
    else:
        up_path = up_raw_path

    orig_file = os.path.basename(orig_path)
    up_file = os.path.basename(up_path)
    _cleanup_preview_cache()
    return jsonify({
        'ok': True,
        'preview_id': token,
        'original_url': f'/api/preview/frame-file/{orig_file}?v={int(time.time())}',
        'upscaled_url': f'/api/preview/frame-file/{up_file}?v={int(time.time())}',
        'time_sec': round(time_sec, 3),
        'effective_scale': upscayl_factor,
    })


@app.route('/api/preview/frame-file/<filename>')
def preview_frame_file(filename):
    safe = os.path.basename(filename)
    if not re.match(r'^[a-f0-9]{12}_(orig|up|up_raw)\.png$', safe):
        return 'Not found', 404
    path = os.path.join(FRAME_PREVIEW_DIR, safe)
    if not os.path.isfile(path):
        return 'Not found', 404
    return send_file(path, mimetype='image/png', conditional=False, max_age=0)


# ── Settings ─────────────────────────────────────────────────────────────────

@app.route('/settings')
def settings_page():
    s = config.load_settings()
    return render_template('settings.html',
                           upscayl_bin=config.UPSCAYL_BIN,
                           upscayl_models_dir=config.UPSCAYL_MODELS_DIR,
                           ffmpeg_bin=config.FFMPEG_BIN,
                           bin_exists=os.path.isfile(config.UPSCAYL_BIN) if config.UPSCAYL_BIN else False,
                           models_exist=os.path.isdir(config.UPSCAYL_MODELS_DIR) if config.UPSCAYL_MODELS_DIR else False,
                           ffmpeg_exists=os.path.isfile(config.FFMPEG_BIN) if config.FFMPEG_BIN else False,
                           project_root=config.BASE_DIR,
                           saved=s,
                           autobin=config._autodetect_bin(),
                           automodels=config._autodetect_models(),
                           platform=platform.system())


@app.route('/settings/save', methods=['POST'])
def settings_save():
    d = request.json or {}
    new_bin      = config._normalize_path(d.get('upscayl_bin', ''))
    new_models   = config._normalize_path(d.get('upscayl_models_dir', ''))
    new_ffmpeg   = config._normalize_path(d.get('ffmpeg_bin', ''))
    use_nvenc    = bool(d.get('use_nvenc', False))
    gpu_device   = int(d.get('gpu_device', 0))
    force_cpu    = bool(d.get('force_cpu', False))
    windows_safe_mode = bool(d.get('windows_safe_mode', False))

    errors = []
    if new_bin and not os.path.isfile(new_bin):
        errors.append(f'Binary not found: {new_bin}')
    if new_models and not os.path.isdir(new_models):
        errors.append(f'Models directory not found: {new_models}')
    if new_ffmpeg and not os.path.isfile(new_ffmpeg):
        errors.append(f'FFmpeg not found: {new_ffmpeg}')
    if errors:
        return jsonify({'ok': False, 'errors': errors}), 400

    new_ffprobe = ''
    if new_ffmpeg:
        candidate_dir = os.path.dirname(new_ffmpeg)
        candidate_probe = os.path.join(candidate_dir, 'ffprobe.exe' if new_ffmpeg.lower().endswith('.exe') else 'ffprobe')
        if os.path.isfile(candidate_probe):
            new_ffprobe = candidate_probe

    config.save_settings({
        'upscayl_bin':        new_bin,
        'upscayl_models_dir': new_models,
        'ffmpeg_bin':         new_ffmpeg,
        'ffprobe_bin':        new_ffprobe,
        'use_nvenc':          use_nvenc,
        'gpu_device':         gpu_device,
        'force_cpu':          force_cpu,
        'windows_safe_mode':  windows_safe_mode,
    })
    config.reload()
    return jsonify({
        'ok':               True,
        'upscayl_bin':      config.UPSCAYL_BIN,
        'upscayl_models_dir': config.UPSCAYL_MODELS_DIR,
        'ffmpeg_bin':       config.FFMPEG_BIN,
        'use_nvenc':        config.USE_NVENC,
        'gpu_device':       config.GPU_DEVICE,
        'force_cpu':        config.FORCE_CPU,
        'windows_safe_mode': config.WINDOWS_SAFE_MODE,
    })


@app.route('/settings/detect', methods=['POST'])
def settings_detect():
    """Re-run auto-detection and return results (without saving)."""
    ffmpeg_bin = config._autodetect_ffmpeg()
    return jsonify({
        'upscayl_bin':        config._autodetect_bin(),
        'upscayl_models_dir': config._autodetect_models(),
        'ffmpeg_bin':         ffmpeg_bin,
        'ffprobe_bin':        config._autodetect_ffprobe(ffmpeg_bin),
        'current_upscayl_bin': config.UPSCAYL_BIN,
        'current_upscayl_models_dir': config.UPSCAYL_MODELS_DIR,
        'current_ffmpeg_bin': config.FFMPEG_BIN,
    })


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    ensure_runtime_ready()
    print(f'\n🎬  VidUpscaler running at http://localhost:{config.PORT}\n')
    app.run(host='0.0.0.0', port=config.PORT, debug=False, threaded=True)

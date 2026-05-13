import json
import os
import platform
import shutil

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

UPLOAD_FOLDER   = os.path.join(BASE_DIR, 'uploads')
FRAMES_FOLDER   = os.path.join(BASE_DIR, 'frames')
UPSCALED_FOLDER = os.path.join(BASE_DIR, 'upscaled')
OUTPUT_FOLDER   = os.path.join(BASE_DIR, 'outputs')
DATABASE        = os.path.join(BASE_DIR, 'vidupscaler.db')
SETTINGS_FILE   = os.path.join(BASE_DIR, 'settings.json')

PORT = 5050
ALLOWED_EXTENSIONS = {'mp4', 'avi', 'mov', 'mkv', 'webm', 'flv', 'm4v'}

# ── Platform-aware auto-detection candidates ───────────────────────────────────
_IS_WIN = platform.system() == 'Windows'
_IS_MAC = platform.system() == 'Darwin'

_BIN_NAME = 'upscayl-bin.exe' if _IS_WIN else 'upscayl-bin'

_UPSCAYL_BIN_CANDIDATES = [
    # macOS – standard .app bundle (confirmed path from source)
    '/Applications/Upscayl.app/Contents/Resources/bin/upscayl-bin',
    '/Applications/Upscayl.app/Contents/MacOS/upscayl-bin',
    # Windows – typical install locations
    r'C:\Program Files\Upscayl\resources\bin\upscayl-bin.exe',
    r'C:\Program Files (x86)\Upscayl\resources\bin\upscayl-bin.exe',
    os.path.join(os.environ.get('LOCALAPPDATA', ''), 'Programs', 'Upscayl', 'resources', 'bin', 'upscayl-bin.exe'),
    # Linux
    '/usr/bin/upscayl-bin',
    '/usr/local/bin/upscayl-bin',
    os.path.join(os.path.expanduser('~'), '.local', 'share', 'upscayl', 'bin', 'upscayl-bin'),
    # PATH fallback
    shutil.which(_BIN_NAME) or '',
    shutil.which('upscayl-bin') or '',
]

_UPSCAYL_MODELS_CANDIDATES = [
    # macOS
    '/Applications/Upscayl.app/Contents/Resources/models',
    os.path.expanduser('~/Library/Application Support/upscayl/models'),
    # Windows
    r'C:\Program Files\Upscayl\resources\models',
    r'C:\Program Files (x86)\Upscayl\resources\models',
    os.path.join(os.environ.get('LOCALAPPDATA', ''), 'Programs', 'Upscayl', 'resources', 'models'),
    os.path.join(os.environ.get('APPDATA', ''), 'upscayl', 'models'),
    # Linux
    '/usr/share/upscayl/models',
    os.path.join(os.path.expanduser('~'), '.local', 'share', 'upscayl', 'models'),
]

def _autodetect_bin():
    return next((p for p in _UPSCAYL_BIN_CANDIDATES if p and os.path.isfile(p)), '')

def _autodetect_models():
    return next((p for p in _UPSCAYL_MODELS_CANDIDATES if p and os.path.isdir(p)), '')

# ── Load user settings (overrides auto-detection) ─────────────────────────────
def load_settings() -> dict:
    if os.path.isfile(SETTINGS_FILE):
        try:
            return json.loads(open(SETTINGS_FILE).read())
        except Exception:
            pass
    return {}

def save_settings(data: dict):
    with open(SETTINGS_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def reload():
    """Re-apply settings from disk into this module's globals."""
    global UPSCAYL_BIN, UPSCAYL_MODELS_DIR, USE_NVENC, GPU_DEVICE, FORCE_CPU
    s = load_settings()
    UPSCAYL_BIN        = s.get('upscayl_bin')        or _autodetect_bin()
    UPSCAYL_MODELS_DIR = s.get('upscayl_models_dir') or _autodetect_models()
    USE_NVENC          = bool(s.get('use_nvenc', False))
    GPU_DEVICE         = int(s.get('gpu_device', 0))
    FORCE_CPU          = bool(s.get('force_cpu', False))

# Initial load
_settings = load_settings()
UPSCAYL_BIN        = _settings.get('upscayl_bin')        or _autodetect_bin()
UPSCAYL_MODELS_DIR = _settings.get('upscayl_models_dir') or _autodetect_models()
USE_NVENC          = bool(_settings.get('use_nvenc', False))
GPU_DEVICE         = int(_settings.get('gpu_device', 0))
FORCE_CPU          = bool(_settings.get('force_cpu', False))

# ── Available models ───────────────────────────────────────────────────────────
UPSCAYL_MODELS = [
    {'id': 'realesrgan-x4plus',       'name': 'Real-ESRGAN x4+ (General)'},
    {'id': 'realesrgan-x4plus-anime', 'name': 'Real-ESRGAN x4+ Anime'},
    {'id': 'realesrnet-x4plus',       'name': 'Real-ESRNet x4+'},
    {'id': 'ultrasharp',              'name': 'UltraSharp'},
    {'id': 'digital-art',             'name': 'Digital Art'},
    {'id': 'remacri',                 'name': 'Remacri'},
    {'id': 'ultramix-balanced',       'name': 'UltraMix Balanced'},
    {'id': 'high-fidelity-4x',        'name': 'High Fidelity 4x'},
]

SCALE_OPTIONS = [2, 3, 4]

# ── Ensure directories exist ───────────────────────────────────────────────────
for _d in (UPLOAD_FOLDER, FRAMES_FOLDER, UPSCALED_FOLDER, OUTPUT_FOLDER):
    os.makedirs(_d, exist_ok=True)

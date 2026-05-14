import json
import os
import platform
import shutil
import glob

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
_IS_LINUX = platform.system() == 'Linux'


def _normalize_path(value: str) -> str:
    """Normalize user-entered paths for the OS running Python."""
    if not value:
        return ''
    p = os.path.expandvars(os.path.expanduser(str(value).strip().strip('"').strip("'")))
    p = p.replace('\\ ', ' ')

    # WSL-style paths are valid on Linux, but not when Python is running on Windows.
    if _IS_WIN and p.startswith('/mnt/'):
        parts = p.split('/')
        if len(parts) > 3 and len(parts[2]) == 1:
            drive = parts[2].upper()
            rest = '\\'.join(parts[3:])
            return f'{drive}:\\{rest}'

    # Windows drive paths need translation only when the app runs inside Linux/WSL.
    if _IS_LINUX and (p.startswith('C:\\') or p.startswith('C:/')):
        return p.replace('\\', '/').replace('C:/', '/mnt/c/').replace('C:\\', '/mnt/c/')

    return os.path.normpath(p)


def _existing_file_or_empty(path: str) -> str:
    p = _normalize_path(path)
    return p if p and os.path.isfile(p) else ''


def _existing_dir_or_empty(path: str) -> str:
    p = _normalize_path(path)
    return p if p and os.path.isdir(p) else ''

_BIN_NAME = 'upscayl-bin.exe' if _IS_WIN else 'upscayl-bin'

# Candidates ordered by priority: current OS first, then others
_mac_bin_candidates = [
    '/Applications/Upscayl.app/Contents/Resources/bin/upscayl-bin',
    '/Applications/Upscayl.app/Contents/MacOS/upscayl-bin',
    '/usr/local/bin/upscayl-bin',
]
_win_bin_candidates = [
    r'C:\Program Files\Upscayl\resources\bin\upscayl-bin.exe',
    r'C:\Program Files (x86)\Upscayl\resources\bin\upscayl-bin.exe',
    os.path.join(os.environ.get('LOCALAPPDATA', ''), 'Programs', 'Upscayl', 'resources', 'bin', 'upscayl-bin.exe'),
    os.path.join(os.path.expanduser('~'), 'AppData', 'Local', 'Microsoft', 'WindowsApps', 'upscayl-bin.exe'),
]
_linux_bin_candidates = [
    '/usr/bin/upscayl-bin',
    '/usr/local/bin/upscayl-bin',
    '/opt/upscayl/bin/upscayl-bin',
    '/snap/upscayl/current/bin/upscayl-bin',
    os.path.join(os.path.expanduser('~'), '.local', 'share', 'upscayl', 'bin', 'upscayl-bin'),
    os.path.join(os.path.expanduser('~'), 'bin', 'upscayl-bin'),
]

_UPSCAYL_BIN_CANDIDATES = (
    (_linux_bin_candidates if _IS_LINUX else []) +
    (_mac_bin_candidates if _IS_MAC else []) +
    (_win_bin_candidates if _IS_WIN else []) +
    # PATH fallback
    [shutil.which(_BIN_NAME) or '', shutil.which('upscayl-bin') or '']
)

# Candidates for models
_mac_models_candidates = [
    '/Applications/Upscayl.app/Contents/Resources/models',
    os.path.expanduser('~/Library/Application Support/upscayl/models'),
]
_win_models_candidates = [
    r'C:\Program Files\Upscayl\resources\models',
    r'C:\Program Files (x86)\Upscayl\resources\models',
    os.path.join(os.environ.get('LOCALAPPDATA', ''), 'Programs', 'Upscayl', 'resources', 'models'),
    os.path.join(os.environ.get('APPDATA', ''), 'upscayl', 'models'),
]
_linux_models_candidates = [
    '/usr/share/upscayl/models',
    '/usr/local/share/upscayl/models',
    '/opt/upscayl/models',
    '/snap/upscayl/current/share/upscayl/models',
    os.path.join(os.path.expanduser('~'), '.local', 'share', 'upscayl', 'models'),
    os.path.join(os.path.expanduser('~'), 'upscayl', 'models'),
]

_UPSCAYL_MODELS_CANDIDATES = (
    (_linux_models_candidates if _IS_LINUX else []) +
    (_mac_models_candidates if _IS_MAC else []) +
    (_win_models_candidates if _IS_WIN else [])
)

def _search_filesystem():
    """Search filesystem for upscayl-bin and models in common locations."""
    bin_candidates = []
    models_candidates = []
    
    if _IS_LINUX:
        # For WSL, search Windows Program Files
        import glob
        windows_program_files = '/mnt/c/Program Files'
        if os.path.isdir(windows_program_files):
            for root, dirs, files in os.walk(windows_program_files):
                if 'upscayl-bin' in files or 'upscayl-bin.exe' in files:
                    bin_path = os.path.join(root, 'upscayl-bin' if 'upscayl-bin' in files else 'upscayl-bin.exe')
                    bin_candidates.append(bin_path)
                    # Assume models are in resources/models relative to bin
                    models_dir = os.path.join(os.path.dirname(bin_path), '..', 'resources', 'models')
                    if os.path.isdir(models_dir):
                        models_candidates.append(models_dir)
                if len(bin_candidates) > 5:  # Limit to avoid too many
                    break
        # Also search Linux paths
        linux_search_paths = ['/usr', '/opt', '/home']
        for base in linux_search_paths:
            if os.path.isdir(base):
                for root, dirs, files in os.walk(base):
                    if 'upscayl-bin' in files:
                        bin_candidates.append(os.path.join(root, 'upscayl-bin'))
                        models_dir = os.path.join(root, '..', 'models')  # Adjust as needed
                        if os.path.isdir(models_dir):
                            models_candidates.append(models_dir)
                    if len(bin_candidates) > 10:
                        break
    
    # Similar for other OS, but for now focus on Linux/WSL
    return bin_candidates[:5], models_candidates[:5]  # Return top 5

# Then in _autodetect_bin, add the searched ones
def _autodetect_bin():
    for p in _UPSCAYL_BIN_CANDIDATES:
        found = _existing_file_or_empty(p)
        if found:
            return found
    # If not found, try filesystem search
    searched_bins, _ = _search_filesystem()
    for p in searched_bins:
        found = _existing_file_or_empty(p)
        if found:
            return found
    return ''

def _autodetect_models():
    for p in _UPSCAYL_MODELS_CANDIDATES:
        found = _existing_dir_or_empty(p)
        if found:
            return found
    # If not found, try filesystem search
    _, searched_models = _search_filesystem()
    for p in searched_models:
        found = _existing_dir_or_empty(p)
        if found:
            return found
    return ''

_FFMPEG_CANDIDATES = [
    shutil.which('ffmpeg') or '',
    os.path.join(os.environ.get('LOCALAPPDATA', ''), 'Microsoft', 'WinGet', 'Packages', 'Gyan.FFmpeg_*', '*', 'bin', 'ffmpeg.exe'),
    os.path.join(os.environ.get('LOCALAPPDATA', ''), 'Microsoft', 'WinGet', 'Links', 'ffmpeg.exe'),
    r'C:\Program Files\Gyan\FFmpeg\bin\ffmpeg.exe',
    r'C:\Program Files\ffmpeg\bin\ffmpeg.exe',
    os.path.join(os.environ.get('LOCALAPPDATA', ''), 'Programs', 'Gyan', 'FFmpeg', 'bin', 'ffmpeg.exe'),
    '/usr/bin/ffmpeg',
    '/usr/local/bin/ffmpeg',
    r'C:\ffmpeg\bin\ffmpeg.exe',
    os.path.join(os.environ.get('ProgramFiles', ''), 'ffmpeg', 'bin', 'ffmpeg.exe'),
]

_FFPROBE_CANDIDATES = [
    shutil.which('ffprobe') or '',
    os.path.join(os.environ.get('LOCALAPPDATA', ''), 'Microsoft', 'WinGet', 'Packages', 'Gyan.FFmpeg_*', '*', 'bin', 'ffprobe.exe'),
    os.path.join(os.environ.get('LOCALAPPDATA', ''), 'Microsoft', 'WinGet', 'Links', 'ffprobe.exe'),
    r'C:\Program Files\Gyan\FFmpeg\bin\ffprobe.exe',
    r'C:\Program Files\ffmpeg\bin\ffprobe.exe',
    os.path.join(os.environ.get('LOCALAPPDATA', ''), 'Programs', 'Gyan', 'FFmpeg', 'bin', 'ffprobe.exe'),
    '/usr/bin/ffprobe',
    '/usr/local/bin/ffprobe',
    r'C:\ffmpeg\bin\ffprobe.exe',
    os.path.join(os.environ.get('ProgramFiles', ''), 'ffmpeg', 'bin', 'ffprobe.exe'),
]

def _autodetect_ffmpeg():
    for p in _FFMPEG_CANDIDATES:
        p = _normalize_path(p)
        if '*' in p:
            # Handle wildcard paths (e.g., for winget packages)
            matches = glob.glob(p)
            for match in matches:
                found = _existing_file_or_empty(match)
                if found:
                    return found
        else:
            found = _existing_file_or_empty(p)
            if found:
                return found
    return ''

def _autodetect_ffprobe(ffmpeg_path: str = ''):
    candidates = []
    if ffmpeg_path:
        ffprobe_dir = os.path.dirname(_normalize_path(ffmpeg_path))
        candidates.extend([
            os.path.join(ffprobe_dir, 'ffprobe'),
            os.path.join(ffprobe_dir, 'ffprobe.exe'),
        ])
    candidates.extend(_FFPROBE_CANDIDATES)
    for p in candidates:
        p = _normalize_path(p)
        if '*' in p:
            # Handle wildcard paths
            matches = glob.glob(p)
            for match in matches:
                found = _existing_file_or_empty(match)
                if found:
                    return found
        else:
            found = _existing_file_or_empty(p)
            if found:
                return found
    return ''

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
    global UPSCAYL_BIN, UPSCAYL_MODELS_DIR, USE_NVENC, GPU_DEVICE, FORCE_CPU, FFMPEG_BIN, FFPROBE_BIN
    s = load_settings()
    UPSCAYL_BIN        = _existing_file_or_empty(s.get('upscayl_bin', '')) or _autodetect_bin()
    UPSCAYL_MODELS_DIR = _existing_dir_or_empty(s.get('upscayl_models_dir', '')) or _autodetect_models()
    FFMPEG_BIN         = _existing_file_or_empty(s.get('ffmpeg_bin', '')) or _autodetect_ffmpeg()
    FFPROBE_BIN        = _existing_file_or_empty(s.get('ffprobe_bin', '')) or _autodetect_ffprobe(FFMPEG_BIN)
    USE_NVENC          = bool(s.get('use_nvenc', False))
    GPU_DEVICE         = int(s.get('gpu_device', 0))
    FORCE_CPU          = bool(s.get('force_cpu', False))

# Initial load
_settings = load_settings()
UPSCAYL_BIN        = _existing_file_or_empty(_settings.get('upscayl_bin', '')) or _autodetect_bin()
UPSCAYL_MODELS_DIR = _existing_dir_or_empty(_settings.get('upscayl_models_dir', '')) or _autodetect_models()
FFMPEG_BIN         = _existing_file_or_empty(_settings.get('ffmpeg_bin', '')) or _autodetect_ffmpeg()
FFPROBE_BIN        = _existing_file_or_empty(_settings.get('ffprobe_bin', '')) or _autodetect_ffprobe(FFMPEG_BIN)
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

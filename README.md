# Viduppy AI

A web application to enhance video quality using Upscayl, an AI-powered upscaling tool.

## Description

This project allows you to upload videos and process them with Upscayl to increase their resolution and quality. It uses Flask as the backend and a simple web interface.

## Prerequisites

Before starting, make sure you have the following components installed:

### 1. Python
- **Recommended version**: Python 3.8 or higher.
- **Download**: Go to [python.org](https://www.python.org/downloads/) and download the latest version for your operating system.
- **Installation**:
  - Windows/macOS: Run the installer and make sure to check "Add Python to PATH".
  - Linux: Use your package manager, for example:
    ```bash
    sudo apt update
    sudo apt install python3 python3-pip
    ```

### 2. Upscayl
Upscayl is the AI tool that performs the upscaling. It must be installed locally.

- **Download**: Go to [upscayl.org/download](https://upscayl.org/download) and download the version for your operating system (Windows, macOS, or Linux).
- **Installation**:
  - Follow the installer instructions.
  - Ensure that the `upscayl-bin` executable is in the PATH or in a known location (the project tries to detect it automatically).

### 3. FFmpeg
FFmpeg is required for video processing, GPU/NVENC detection, and audio analysis via `ffprobe`.

- **Windows**:
  - Download from [ffmpeg.org](https://ffmpeg.org/download.html#build-windows).
  - Add the `bin` directory to the PATH, or provide the full path to `ffmpeg.exe` in the app settings.

- **macOS**:
  - Install with Homebrew: `brew install ffmpeg`

- **Linux**:
  - Ubuntu/Debian: `sudo apt install ffmpeg`
  - CentOS/RHEL: `sudo yum install ffmpeg` or `sudo dnf install ffmpeg`

If `ffmpeg` is not available on PATH, go to the app settings and enter the full executable path in the FFmpeg field.
## Installation

1. **Clone or download the repository**:
   ```bash
   git clone <repository-url>
   cd vid-upscaler-ai
   ```

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Initialize the database**:
   The SQLite database is created automatically when running the application for the first time. Simply run:
   ```bash
   python run.py
   ```
   This will create the `vidupscaler.db` file in the project directory.

## Configuration
/mnt/c/Program\ Files/Upscayl/resources/bin/upscayl-bin.exe
- The project tries to automatically detect the location of Upscayl and its models by searching common default installation paths for Windows, macOS, and Linux.
- If not detected, you can configure manually by editing `settings.json` or through the web interface at `/settings`.
- Automatically created folders:
  - `uploads/`: For uploaded videos.
  - `frames/`: For extracted frames.
  - `upscaled/`: For processed frames.
  - `outputs/`: For final videos.

## Running the Application

1. Run the server:
   ```bash
   python run.py
   ```

2. Open your browser and go to `http://localhost:5050`.

3. Upload a video, select upscaling options, and process.

## Additional Notes

- Make sure Upscayl is working correctly before using the application (test with the Upscayl interface directly).
- For large videos, processing may take time depending on your hardware.
- The application uses a background thread to process the job queue.
- If you encounter issues with Upscayl detection, check the paths in `config.py` or configure manually in `settings.json`.

## UI Language (i18n)

The app includes a language selector in **Settings**.

- Built-in languages: `English` (`en`) and `Español` (`es`).
- The selected language is saved in `localStorage` using the key `vidupscaler_lang`.
- The language list in Settings is generated automatically from the `I18N` object keys in `templates/app.html`.

### Add a new language

1. Open `templates/app.html`.
2. Find the `I18N` constant in the `<script>` section.
3. Add a new language block (for example `fr`):
   ```js
   const I18N = {
     en: { /* ... */ },
     es: { /* ... */ },
     fr: {
       'active.no_job': 'Aucun job actif',
       'queue.active_title': 'File active',
       // add all keys used by your UI
     },
   };
   ```
4. (Optional) Add a friendly display name in `LANGUAGE_LABELS`:
   ```js
   const LANGUAGE_LABELS = {
     en: 'English',
     es: 'Español',
     fr: 'Français',
   };
   ```
5. Reload the app. The new language will appear automatically in **Settings → UI language**.

### Notes for contributors

- Keep translation keys consistent across all languages.
- If a key is missing in the selected language, the app falls back to English.

## Support

If you have problems, check:
- That Python is in the PATH.
- That Upscayl is installed and accessible.
- That FFmpeg is installed.

For more help, check the logs in the console when running `python run.py`.

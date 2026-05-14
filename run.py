"""
Entry point for VidUpscaler.
Run with: python run.py
"""
from database import init_db
from app import app, worker, config


if __name__ == '__main__':
    init_db()
    worker.start()
    print(f'\nVidUpscaler running at http://localhost:{config.PORT}')
    print(f'   Upscayl binary : {config.UPSCAYL_BIN or "NOT FOUND - open Settings"}')
    print(f'   Models dir     : {config.UPSCAYL_MODELS_DIR or "NOT FOUND - open Settings"}')
    print(f'   FFmpeg binary  : {config.FFMPEG_BIN or "NOT FOUND - open Settings"}')
    print()
    app.run(host='0.0.0.0', port=config.PORT, debug=False, threaded=True)

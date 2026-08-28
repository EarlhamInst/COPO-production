import platform
import shutil
import subprocess

from common.utils.helpers import get_env

# Configuration
COMPOSE = 'docker compose'
DEBUG_PORT = int(get_env('PLAYWRIGHT_DEBUG_PORT'))
SERVICE = 'playwright'
USAGE = f'Usage: copo test {SERVICE} <run|debug|restart>'
VNC_PORT = int(get_env('VNC_PORT'))


# Utility functions
def run(cmd):
    subprocess.run(cmd, shell=True, check=True)


def install_vnc():
    if shutil.which('vncviewer'):
        print('TigerVNC already installed.')
        return

    print('TigerVNC not installed. Installing...')

    os_name = platform.system()

    if os_name == 'Linux':
        run('sudo apt update')
        run('sudo apt install -y tigervnc-viewer')
    elif os_name == 'Darwin':
        run('brew install --cask tigervnc-viewer')
    elif os_name == 'Windows':
        print('Windows detected.')
        print('Please install TigerVNC Viewer manually from:')
        print('https://www.realvnc.com/en/connect/download/viewer-2/')
    else:
        raise RuntimeError(f'Unsupported OS: {os_name}')


def start_vnc():
    # Ensure TigerVNC exists and open viewer.
    install_vnc()

    print(f'Opening VNC viewer on localhost:{VNC_PORT}')

    subprocess.Popen(
        ['vncviewer', f'localhost:{VNC_PORT}'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

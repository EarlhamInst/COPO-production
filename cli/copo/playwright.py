from common.utils.helpers import get_env
from .helpers import (
    run,
    start_vnc,
    COMPOSE,
    DEBUG_PORT,
    SERVICE,
    USAGE,
    VNC_PORT,
)

# Playwright commands
def playwright_restart():
    run(f'{COMPOSE} down')
    run(f'{COMPOSE} up -d {SERVICE}')

def playwright_debug():
    ''' 
    This is where the tests are done locally on on one's machine 
    with the ability to debug and see the browser via VNC
    '''
    print('Running Playwright tests in debug mode...')

    run(f'''
    {COMPOSE} run --rm \
    -p {DEBUG_PORT}:{DEBUG_PORT} \
    -p {VNC_PORT}:{VNC_PORT} \
    {SERVICE} \
    python -m debugpy \
      --listen 0.0.0.0:{DEBUG_PORT} \
      --wait-for-client \
      -m pytest test/{SERVICE} -s
    ''')

    start_vnc()


def playwright_run():
    '''
    This is where continuous integration (CI) tests are done using
    headless tests and CI tools like GitHub Actions
    '''
    print('Running Playwright tests in CLI mode...')
    run(f'{COMPOSE} up -d {SERVICE}')
    run(f"{COMPOSE} exec {SERVICE} pytest test/{SERVICE}")


def playwright_cmd(args):
    if not args:
        print(USAGE)
        return

    cmd = args[0]

    match cmd:
        case 'run':
            return playwright_run()
        case 'debug':
            return playwright_debug()
        case 'restart':
            return playwright_restart()
        case _:
            return f"Unknown {SERVICE} command: {cmd}. Use 'run', 'debug' or 'restart'."

import sys

from .helpers import SERVICE, USAGE
from .playwright import playwright_cmd


def main():
    args = sys.argv[1:]  # e.g. ['playwright', 'run']
    if len(args) < 1:
        print(USAGE)
        sys.exit(1)

    service = args[0]

    if service == 'playwright':
        cmd = args[1:]
        playwright_cmd(cmd)
    else:
        print(f'Unknown command: {service}.')
        sys.exit(1)

import os
import requests

ENA_REQUEST_TIMEOUT = (30, 300) # Seconds to wait for connection and response
WEBIN_CLI_PASSWORD_ENV = 'WEBIN_CLI_PASSWORD'
WEBIN_CLI_PASSWORD_OPTION = f'-passwordEnv={WEBIN_CLI_PASSWORD_ENV}'


def post_xml_files(url, user, password, xml_files, timeout=ENA_REQUEST_TIMEOUT):
    files = {}
    try:
        for name, path in xml_files.items():
            files[name] = (os.path.basename(path), open(path, 'rb'), 'application/xml')
        return requests.post(url, auth=(user, password), files=files, timeout=timeout)
    finally:
        for _, handle, _ in files.values():
            handle.close()


def get(url, user, password, timeout=ENA_REQUEST_TIMEOUT, **kwargs):
    # This `requests` command is similar to the curl command -  `curl -u user:password url`.
    return requests.get(url, auth=(user, password), timeout=timeout, **kwargs)


def webin_cli_env(password):
    # Environment for a Webin-CLI process run with `WEBIN_CLI_PASSWORD_OPTION`.
    env = dict(os.environ)
    env[WEBIN_CLI_PASSWORD_ENV] = password
    return env

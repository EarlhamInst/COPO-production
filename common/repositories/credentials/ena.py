"""ENA (Webin) credential provider.

Webin credentials are basic-auth: a username of the form `Webin-NNNNN@domain`
plus a password. Several call sites decompose the username into a token
(before '@') and a domain (after '@'); this provider centralises that split so
the shape lives in one place and both user-supplied and COPO-default
credentials come out identical.
"""

from common.repositories.credentials.base import (
    CredentialField,
    RepositoryCredentialProvider,
    register,
)
from common.utils.helpers import get_env

ENA_WEBIN_AUTH_URL = "https://www.ebi.ac.uk/ena/submit/webin/auth/token?ttl=1"


def _decompose(webin_user: str, password: str) -> dict:
    """Normalise a Webin username + password into the canonical cred dict.

    Every consumer downstream expects these four keys, so producing them here
    means the `.split("@")` logic is never repeated at a call site again.
    """
    webin_user = (webin_user or "").strip()
    token, _, domain = webin_user.partition("@")
    return {
        "webin_user": webin_user,   # full "Webin-NNNNN@domain"
        "user_token": token,        # portion before '@'
        "webin_domain": domain,     # portion after '@'
        "password": password or "",
    }


@register
class EnaCredentialProvider(RepositoryCredentialProvider):
    key = "ena"
    label = "ENA (Webin)"
    fields = [
        CredentialField(
            name="webin_user",
            label="Webin username",
            help_text="Your ENA Webin submission account, e.g. Webin-12345@example.org",
        ),
        CredentialField(name="webin_password", label="Webin password", secret=True),
    ]

    def normalize(self, values: dict) -> dict:
        return _decompose(values.get("webin_user"), values.get("webin_password"))

    def default_credentials(self):
        webin_user = get_env("WEBIN_USER")
        password = get_env("WEBIN_USER_PASSWORD")
        if not webin_user or not password:
            return None
        return _decompose(webin_user, password)

    def validate(self, values: dict) -> tuple:
        """Check ENA Webin credentials against the live service.

        `values` holds the raw form input: {"webin_user", "webin_password"}.
        Return (ok: bool, message: str).
        """
        import requests as r
        creds = _decompose(values.get("webin_user"), values.get("webin_password"))

        # Confirm `creds["webin_user"]` + `creds["password"]` are accepted by
        # Webin, and return (True, "...") on success or (False, "<reason>")
        url = ENA_WEBIN_AUTH_URL
        resp = r.post(url, json={"authRealms": ["ENA"], "password": creds["password"], "username": creds["webin_user"]})
        if resp.status_code == 200:
            return (True, "ENA Webin credentials are valid.")
        else:
            return (False, f"ENA Webin credentials are invalid: {resp.status_code} {resp.text}")

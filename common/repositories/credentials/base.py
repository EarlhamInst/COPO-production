"""Provider abstraction + resolver for per-user repository credentials.

The design mirrors the LIMSAdapter pattern already in the codebase: an ABC,
a registry, and one concrete class per backend. Each provider declares the
*fields* it needs, how to *validate* them against the live repository, and how
to read COPO's own *default* credentials (today: environment variables).

Resolution order for a submission:
    1. the user's own stored credentials for this repo, if present
    2. COPO's shared default credentials (the last link in the chain)

Accountability rule: the COPO-default fallback applies only when a user has
NOT supplied their own credentials. A user who *has* supplied credentials that
then fail must get a hard error, never a silent downgrade onto COPO's shared
account — see resolve().
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from django.utils import timezone


class CredentialResolutionError(Exception):
    """Raised when no usable credentials can be resolved for a submission."""


@dataclass(frozen=True)
class CredentialField:
    """One input a repository needs. Drives form rendering and encryption."""

    name: str
    label: str
    secret: bool = False  # rendered as a password input; encrypted at rest
    help_text: str = ""


@dataclass
class ResolvedCredentials:
    """The credentials to use for a submission, plus where they came from."""

    repo_key: str
    source: str  # "user" or "copo_default"
    values: dict = field(default_factory=dict)


# --- registry --------------------------------------------------------------

_REGISTRY: dict = {}


def register(provider_cls):
    """Class decorator that adds a provider to the registry under its key."""
    instance = provider_cls()
    _REGISTRY[instance.key] = instance
    return provider_cls


def get_provider(repo_key: str) -> "RepositoryCredentialProvider":
    try:
        return _REGISTRY[repo_key]
    except KeyError:
        raise CredentialResolutionError(
            f"No credential provider registered for repository '{repo_key}'."
        )


def all_providers() -> list:
    return list(_REGISTRY.values())


# --- provider ABC ----------------------------------------------------------

class RepositoryCredentialProvider(ABC):
    key: str = ""            # matches the repository=="..." branches (e.g. "ena")
    label: str = ""          # human-facing name for the UI
    fields: list = []        # list[CredentialField]

    def normalize(self, values: dict) -> dict:
        """Map raw form input to the canonical shape used everywhere else.

        Default is identity; providers override when their stored/resolved
        shape differs from the form fields (e.g. ENA derives token + domain
        from the Webin username). Keeps user-supplied and COPO-default
        credentials structurally identical for downstream consumers.
        """
        return dict(values)

    @abstractmethod
    def default_credentials(self) -> Optional[dict]:
        """COPO's shared default credentials, or None if none are configured."""

    @abstractmethod
    def validate(self, values: dict) -> tuple:
        """Check credentials against the live repository.

        Returns (ok: bool, message: str).
        """

    def resolve(self, user, prefer_default: bool = False) -> ResolvedCredentials:
        """Resolve which credentials a submission should use.

        prefer_default=True is the popup's "use COPO default" choice — a
        one-off override that skips the user's own creds for this submission.
        """
        if not prefer_default:
            own = load_user_credentials(user, self.key)
            if own is not None:
                # The user supplied their own creds: use them, and never fall
                # back to COPO's account if they turn out to be wrong. That is
                # the accountability rule — a silent downgrade would submit a
                # user's data under COPO's identity without their knowledge.
                return ResolvedCredentials(
                    repo_key=self.key, source="user", values=own
                )

        default = self.default_credentials()
        if default is None:
            raise CredentialResolutionError(
                f"No COPO default credentials configured for '{self.key}'."
            )
        return ResolvedCredentials(
            repo_key=self.key, source="copo_default", values=default
        )


# --- storage (encrypted at rest) ------------------------------------------

def _secret_field_names(provider: RepositoryCredentialProvider) -> set:
    return {f.name for f in provider.fields if f.secret}


def save_user_credentials(user, repo_key: str, values: dict,
                          validate: bool = True) -> tuple:
    """Persist a user's own credentials for a repo, encrypting secret fields.

    Returns (ok: bool, message: str). When validate=True the credentials are
    checked against the live repository before the row is marked valid.
    """
    import json
    from common.utils import credential_crypto
    from src.apps.copo_core.models import UserRepositoryCredential

    provider = get_provider(repo_key)

    ok, message = (True, "")
    if validate:
        ok, message = provider.validate(values)

    # Store the canonical shape so user creds and COPO defaults are identical
    # to every downstream consumer.
    canonical = provider.normalize(values)
    payload = credential_crypto.encrypt(json.dumps(canonical))
    UserRepositoryCredential.objects.update_or_create(
        user=user,
        repo_key=repo_key,
        defaults={
            "payload": payload,
            "is_valid": ok,
            "validated_at": timezone.now() if validate else None,
        },
    )
    return ok, message


def load_user_credentials(user, repo_key: str) -> Optional[dict]:
    """Return the user's decrypted credential values for a repo, or None."""
    import json
    from common.utils import credential_crypto
    from src.apps.copo_core.models import UserRepositoryCredential

    row = UserRepositoryCredential.objects.filter(
        user=user, repo_key=repo_key
    ).first()
    if row is None or not row.payload:
        return None
    return json.loads(credential_crypto.decrypt(row.payload))


def resolve(user, repo_key: str, prefer_default: bool = False) -> ResolvedCredentials:
    """Module-level convenience wrapper around provider.resolve()."""
    return get_provider(repo_key).resolve(user, prefer_default=prefer_default)

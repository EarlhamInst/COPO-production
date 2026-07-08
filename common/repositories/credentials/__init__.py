"""Pluggable per-user credential resolution for submission repositories.

Public surface:
    get_provider(repo_key)         -> RepositoryCredentialProvider
    resolve(user, repo_key, ...)   -> ResolvedCredentials
    save_user_credentials(...)     -> persist + (optionally) validate a user's own creds
"""

from common.repositories.credentials.base import (
    CredentialField,
    RepositoryCredentialProvider,
    ResolvedCredentials,
    CredentialResolutionError,
    get_provider,
    all_providers,
    resolve,
    save_user_credentials,
    load_user_credentials,
)

# Importing the concrete providers registers them via @register.
from common.repositories.credentials import ena  # noqa: F401

__all__ = [
    "CredentialField",
    "RepositoryCredentialProvider",
    "ResolvedCredentials",
    "CredentialResolutionError",
    "get_provider",
    "all_providers",
    "resolve",
    "save_user_credentials",
    "load_user_credentials",
]

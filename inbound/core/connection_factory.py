"""Factory for creating connections."""

from typing import Callable, Dict

from inbound.core.models import Profile
from inbound.plugins.connections.connection import Connection

connection_creation_funcs: Dict[str, Callable[..., Connection]] = {}


def register(connection_type: str, creator_fn: Callable[..., Connection]) -> None:
    """Register a connection type."""
    connection_creation_funcs[connection_type] = creator_fn


def unregister(connection_type: str) -> None:
    """Unregister a connection type."""
    connection_creation_funcs.pop(connection_type, None)


def create(profile: Profile) -> Connection:
    """Create a connection of a specific type."""
    connection_type = profile.type
    try:
        creator_func = connection_creation_funcs[connection_type]
    except KeyError:
        raise ValueError(f"unknown connection type {str(connection_type)}") from None
    return creator_func(profile.copy())

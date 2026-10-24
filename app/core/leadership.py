"""Lease context shared by singleton services without importing their supervisor."""

import time
from dataclasses import dataclass


class LeadershipLost(Exception):
    """The caller no longer owns a valid leader lease."""


@dataclass
class LeaderLease:
    """Track one acquisition token and its conservative local deadline."""

    value: str
    # This is a local monotonic deadline, not a Unix timestamp or Redis expiry time.
    deadline: float
    active: bool = True

    @property
    def valid(self) -> bool:
        """Require active ownership and an unexpired monotonic deadline."""
        return self.active and time.monotonic() < self.deadline

    def check(self) -> None:
        """Stop protected work when local leadership is no longer valid."""
        if not self.valid:
            raise LeadershipLost("leader lease expired or relinquished")

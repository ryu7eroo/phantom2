from __future__ import annotations

from dataclasses import dataclass

from .models import Candidate


class Verifier:
    """Verification policy boundary; real CTF-specific checks are adapters."""

    async def verify(self, candidate: Candidate) -> bool:
        # V1 deliberately requires a non-empty candidate and proof.
        # Network/service interaction belongs in a later sandbox adapter.
        return bool(candidate.value.strip()) and bool(candidate.proof)

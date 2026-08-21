from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .models import Evidence, TaskState


@dataclass
class Blackboard:
    state: TaskState

    def add_evidence(self, item: Evidence) -> None:
        self.state.evidence.append(item)
        self.state.dead_ends.update(item.failed_paths)

    def unexplored(self, proposal: str) -> bool:
        return proposal not in self.state.dead_ends

    def context(self) -> dict[str, object]:
        return {
            "round": self.state.round.value,
            "dead_ends": sorted(self.state.dead_ends),
            "evidence": [
                {
                    "agent_id": e.agent_id,
                    "claim": e.claim,
                    "evidence": list(e.evidence),
                    "confidence": e.confidence,
                }
                for e in self.state.evidence
            ],
        }

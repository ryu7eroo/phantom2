from __future__ import annotations

from dataclasses import dataclass

from .models import Evidence, TaskState


@dataclass
class Blackboard:
    state: TaskState

    def add_evidence(self, item: Evidence) -> None:
        self.state.evidence.append(item)
        self.state.dead_ends.update(item.failed_paths)
        self.state.explored_paths.update(item.tested_paths)
        self.state.open_hypotheses.update(item.next_hypotheses)
        # A path can no longer be considered open once it has been disproved.
        self.state.open_hypotheses.difference_update(item.failed_paths)

    def unexplored(self, proposal: str) -> bool:
        return proposal not in self.state.dead_ends and proposal not in self.state.explored_paths

    def available_hypotheses(self) -> tuple[str, ...]:
        return tuple(sorted(self.state.open_hypotheses - self.state.dead_ends))

    def context(self) -> dict[str, object]:
        return {
            "round": self.state.round.value,
            "dead_ends": sorted(self.state.dead_ends),
            "explored_paths": sorted(self.state.explored_paths),
            "open_hypotheses": self.available_hypotheses(),
            "evidence": [
                {
                    "agent_id": e.agent_id,
                    "claim": e.claim,
                    "evidence": list(e.evidence),
                    "tested_paths": list(e.tested_paths),
                    "failed_paths": list(e.failed_paths),
                    "next_hypotheses": list(e.next_hypotheses),
                    "confidence": e.confidence,
                    "round": e.round.value,
                }
                for e in self.state.evidence
            ],
        }

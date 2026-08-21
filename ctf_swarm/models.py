from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Round(Enum):
    INDEPENDENT = "independent"
    COLLABORATIVE = "collaborative"
    DIVERGENT = "divergent"
    ADVERSARIAL = "adversarial"
    VERIFY = "verify"
    SOLVED = "solved"


class EventType(Enum):
    TASK_STARTED = "task_started"
    HYPOTHESIS = "hypothesis"
    CANDIDATE = "candidate"
    VERIFIED = "verified"
    REJECTED = "rejected"
    ROUND_ADVANCED = "round_advanced"
    GLOBAL_CANCEL = "global_cancel"
    EVIDENCE_MERGED = "evidence_merged"
    CRITIQUE = "critique"


@dataclass(frozen=True)
class Challenge:
    id: str
    title: str
    category: str
    points: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Evidence:
    agent_id: str
    challenge_id: str
    claim: str
    evidence: tuple[str, ...] = ()
    tested_paths: tuple[str, ...] = ()
    failed_paths: tuple[str, ...] = ()
    next_hypotheses: tuple[str, ...] = ()
    confidence: float = 0.0
    round: Round = Round.INDEPENDENT


@dataclass(frozen=True)
class Candidate:
    agent_id: str
    challenge_id: str
    value: str
    proof: tuple[str, ...] = ()
    confidence: float = 0.0


@dataclass
class TaskState:
    challenge: Challenge
    round: Round = Round.INDEPENDENT
    active_agents: set[str] = field(default_factory=set)
    evidence: list[Evidence] = field(default_factory=list)
    candidates: list[Candidate] = field(default_factory=list)
    dead_ends: set[str] = field(default_factory=set)
    explored_paths: set[str] = field(default_factory=set)
    open_hypotheses: set[str] = field(default_factory=set)
    solved_value: str | None = None
    solved_by: str | None = None
    cancelled: bool = False

    def is_solved(self) -> bool:
        return self.solved_value is not None

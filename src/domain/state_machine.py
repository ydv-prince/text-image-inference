"""
Pure Domain State Machine & Transition Invariants
Location: src/domain/state_machine.py

Rules:
1. State transitions are strictly validated.
2. Terminal states (COMPLETED, FAILED, CANCELLED, TIMED_OUT) accept no further transitions.
3. Invalid transitions raise InvalidStateTransitionException.
"""

from typing import Dict, Optional, Set

from src.core.errors import InvalidStateTransitionException
from src.domain.models import JobStatus


class JobStateMachine:
    """
    Enforces valid state machine transitions for GenerationJob entities.
    """

    VALID_TRANSITIONS: Dict[JobStatus, Set[JobStatus]] = {
        JobStatus.PENDING: {
            JobStatus.PROCESSING,
            JobStatus.CANCELLED,
        },
        JobStatus.PROCESSING: {
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.PENDING,    # Safe retry
            JobStatus.TIMED_OUT,  # Expired lease
            JobStatus.CANCELLED,
        },
        # Terminal states
        JobStatus.COMPLETED: set(),
        JobStatus.FAILED: set(),
        JobStatus.CANCELLED: set(),
        JobStatus.TIMED_OUT: set(),
    }

    @classmethod
    def can_transition(cls, from_status: JobStatus, to_status: JobStatus) -> bool:
        """Returns True if the transition from from_status to to_status is permissible."""
        return to_status in cls.VALID_TRANSITIONS.get(from_status, set())

    @classmethod
    def validate_transition(
        cls,
        from_status: JobStatus,
        to_status: JobStatus,
        job_id: Optional[str] = None,
    ) -> None:
        """
        Validates transition and raises InvalidStateTransitionException if illegal.
        """
        if not cls.can_transition(from_status, to_status):
            raise InvalidStateTransitionException(
                current_state=from_status.value,
                target_state=to_status.value,
                job_id=job_id,
            )

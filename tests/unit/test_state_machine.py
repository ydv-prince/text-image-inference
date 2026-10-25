"""
Unit Tests for Pure Domain State Machine
Location: tests/unit/test_state_machine.py
"""

import pytest

from src.core.errors import InvalidStateTransitionException
from src.domain.models import JobStatus
from src.domain.state_machine import JobStateMachine


def test_valid_transitions():
    """Verifies that all legal domain state transitions succeed."""
    # From PENDING
    assert JobStateMachine.can_transition(JobStatus.PENDING, JobStatus.PROCESSING)
    assert JobStateMachine.can_transition(JobStatus.PENDING, JobStatus.CANCELLED)

    # From PROCESSING
    assert JobStateMachine.can_transition(JobStatus.PROCESSING, JobStatus.COMPLETED)
    assert JobStateMachine.can_transition(JobStatus.PROCESSING, JobStatus.FAILED)
    assert JobStateMachine.can_transition(JobStatus.PROCESSING, JobStatus.PENDING)  # Retry
    assert JobStateMachine.can_transition(JobStatus.PROCESSING, JobStatus.TIMED_OUT)
    assert JobStateMachine.can_transition(JobStatus.PROCESSING, JobStatus.CANCELLED)


def test_invalid_transitions_raise_exception():
    """Verifies that illegal state transitions raise InvalidStateTransitionException."""
    # Cannot jump from PENDING directly to COMPLETED
    with pytest.raises(InvalidStateTransitionException) as exc_info:
        JobStateMachine.validate_transition(JobStatus.PENDING, JobStatus.COMPLETED, job_id="test-job-1")
    assert "Cannot transition job (test-job-1) from 'PENDING' to 'COMPLETED'" in str(exc_info.value)

    # Cannot transition from terminal states
    terminal_states = [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.TIMED_OUT]
    for terminal in terminal_states:
        for target in JobStatus:
            assert not JobStateMachine.can_transition(terminal, target)
            with pytest.raises(InvalidStateTransitionException):
                JobStateMachine.validate_transition(terminal, target)

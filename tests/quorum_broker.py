"""Internal quorum broker -- **not** part of the upstream contribution.

The implementation now lives in the shipped package at
:mod:`dramatiq.brokers.rabbitmq_quorum` (so it is importable from Intezer's
internal ``dramatiq`` build); this module just re-exports it so the existing
tests keep referencing ``tests.quorum_broker``.
"""

from __future__ import annotations

from dramatiq.brokers.rabbitmq_quorum import QuorumRabbitmqBroker

__all__ = ["QuorumRabbitmqBroker"]

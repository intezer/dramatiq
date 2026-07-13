"""Internal quorum broker -- **not** part of the upstream contribution.

The upstream change is the ``consumer_timeout`` re-lease on
:class:`RabbitmqBroker` alone; this subclass lives on an internal branch and
relies only on the base class's overridable surface --
``_build_queue_arguments``, ``_declare_dq_queue``, ``_declare_xq_queue`` and
``consumer_timeout`` -- so the base fix can go upstream unchanged.
"""

from __future__ import annotations

from typing import Any, Optional

from dramatiq.brokers.rabbitmq import DEAD_MESSAGE_TTL, MIN_CONSUMER_TIMEOUT, RabbitmqBroker
from dramatiq.common import dq_name, xq_name


class QuorumRabbitmqBroker(RabbitmqBroker):
    """A broker for RabbitMQ quorum queues.  Requires **RabbitMQ 4.3+**.

    Reuses the entire delay/retry machinery of :class:`RabbitmqBroker` --
    including the ``consumer_timeout`` re-lease -- and only changes how the
    underlying queues are declared.
    """

    def __init__(
        self,
        *,
        delivery_limit: Optional[int] = None,
        consumer_timeout: Optional[int] = MIN_CONSUMER_TIMEOUT,
        **kwargs: Any,
    ) -> None:
        if kwargs.get("max_priority") is not None:
            raise RuntimeError(
                "max_priority is not supported by quorum queues; they always provide the "
                "full 0-31 priority range (send with the broker_priority option instead)."
            )

        self.delivery_limit = delivery_limit
        super().__init__(consumer_timeout=consumer_timeout, **kwargs)

    def _build_queue_arguments(self, queue_name):
        arguments = {
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": xq_name(queue_name),
        }
        if self.delivery_limit is not None:
            arguments["x-delivery-limit"] = self.delivery_limit

        return arguments

    def _declare_dq_queue(self, queue_name):
        arguments = self._build_queue_arguments(queue_name)
        # The delay queue holds messages unacked in memory until their eta
        # passes, so the broker must never cap their (re)deliveries -- the
        # acknowledgement timeout would otherwise dead-letter them early.
        arguments["x-delivery-limit"] = -1
        return self.channel.queue_declare(queue=dq_name(queue_name), durable=True, arguments=arguments)

    def _declare_xq_queue(self, queue_name):
        return self.channel.queue_declare(
            queue=xq_name(queue_name),
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                # This HAS to be a static value since messages are expired
                # in order inside of RabbitMQ (head-first).
                "x-message-ttl": DEAD_MESSAGE_TTL,
            },
        )

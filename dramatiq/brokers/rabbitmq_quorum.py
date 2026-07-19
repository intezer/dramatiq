# This file is a part of Dramatiq.
#
# Copyright (C) 2017,2018 CLEARTYPE SRL <bogdan@cleartype.io>
#
# Dramatiq is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# Dramatiq is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Internal RabbitMQ quorum-queue broker -- **not** part of the upstream
contribution.

The upstream change is the ``consumer_timeout`` re-lease on
:class:`~dramatiq.brokers.rabbitmq.RabbitmqBroker` alone; this subclass ships
only in Intezer's internal ``dramatiq`` build and relies solely on the base
class's overridable surface -- ``_build_queue_arguments``, ``_declare_dq_queue``,
``_declare_xq_queue`` and ``consumer_timeout`` -- so the base fix stays
upstream-compatible.
"""

from __future__ import annotations

from typing import Any, Optional

from ..common import dq_name, xq_name
from .rabbitmq import DEAD_MESSAGE_TTL, MIN_CONSUMER_TIMEOUT, RabbitmqBroker


class QuorumRabbitmqBroker(RabbitmqBroker):
    """A broker for RabbitMQ quorum queues.  Requires **RabbitMQ 4.3+**.

    Reuses the entire delay/retry machinery of :class:`RabbitmqBroker` --
    including the ``consumer_timeout`` re-lease -- and only changes how the
    underlying queues are declared.

    Unlike :class:`RabbitmqBroker`, ``confirm_delivery`` defaults to ``True``:
    a quorum queue only acknowledges a publish once it has been committed to a
    majority of replicas, so fire-and-forget publishing would silently lose
    messages enqueued while the queue has lost quorum.  With confirms on, such a
    publish blocks and then raises instead of reporting a false success.
    """

    def __init__(
        self,
        *,
        delivery_limit: Optional[int] = None,
        consumer_timeout: Optional[int] = MIN_CONSUMER_TIMEOUT,
        confirm_delivery: bool = True,
        **kwargs: Any,
    ) -> None:
        if kwargs.get("max_priority") is not None:
            raise RuntimeError(
                "max_priority is not supported by quorum queues; they always provide the "
                "full 0-31 priority range (send with the broker_priority option instead)."
            )

        self.delivery_limit = delivery_limit
        # Quorum queues confirm a publish only after a majority commit, so
        # default confirm_delivery to True: fire-and-forget would silently
        # drop publishes made while the queue has lost quorum (the whole
        # failure mode this broker exists to survive).
        super().__init__(confirm_delivery=confirm_delivery, consumer_timeout=consumer_timeout, **kwargs)

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

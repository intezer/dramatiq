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
only in Intezer's internal ``dramatiq`` build and relies on the base class's
overridable surface -- ``_build_queue_arguments``, ``_declare_dq_queue``,
``_declare_xq_queue``, ``consume`` and ``consumer_timeout`` -- plus a logging
filter it installs on the worker's per-queue logger to downgrade the
connection-error record for a deliberate broker restart.  It changes no base
code, so the base fix stays upstream-compatible.
"""

from __future__ import annotations

import logging
import random
from typing import Any, Optional

from ..broker import Consumer
from ..common import dq_name, xq_name
from ..logging import get_logger
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

    @property
    def parameters(self):
        # Shuffle per read so every (re)connection picks a random broker
        # instead of always pinning to the first configured one.
        if isinstance(self._parameters, list):
            return random.sample(self._parameters, len(self._parameters))
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        self._parameters = value

    def consume(self, queue_name: str, prefetch: int = 1, timeout: int = 5000) -> Consumer:
        # The worker logs a per-queue critical when the broker deliberately
        # closes the connection (a recoverable restart/deploy).  Filters only
        # run on the originating logger, so attach ours to this queue's worker
        # logger here, where the consuming queue name is known.  Local import
        # avoids any cycle and reads the worker module's own logger prefix.
        from .. import worker

        logger = get_logger(worker.__name__, "ConsumerThread(%s)" % queue_name)
        if not any(isinstance(f, _DowngradeExpectedDisconnects) for f in logger.filters):
            logger.addFilter(_DowngradeExpectedDisconnects())
        return super().consume(queue_name, prefetch, timeout)

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


class _DowngradeExpectedDisconnects(logging.Filter):
    """Downgrade the worker's consumer connection-error record to WARNING when
    the disconnect was one the broker initiated deliberately (AMQP 320
    CONNECTION_FORCED, e.g. a rolling restart or deploy), from which the
    consumer recovers by reconnecting.  Genuine faults -- a broker that stays
    down, a 404, an abrupt SIGKILL (StreamLostError, no reply code) -- do not
    carry that signature and keep their original level.

    This is the quorum-broker-only replacement for a ``BrokerShutdown``
    exception in the base broker, which is unlikely to be accepted upstream.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno > logging.WARNING and "CONNECTION_FORCED" in record.getMessage():
            record.levelno = logging.WARNING
            record.levelname = "WARNING"
        return True

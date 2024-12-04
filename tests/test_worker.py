import dramatiq
from dramatiq.middleware import middleware
from .common import worker


def test_workers_dont_register_queues_that_arent_whitelisted(stub_broker):
    # Given that I have a worker object with a restricted set of queues
    with worker(stub_broker, queues={"a", "b"}) as stub_worker:
        # When I try to register a consumer for a queue that hasn't been whitelisted
        stub_broker.declare_queue("c")
        stub_broker.declare_queue("c.DQ")

        # Then a consumer should not get spun up for that queue
        assert "c" not in stub_worker.consumers
        assert "c.DQ" not in stub_worker.consumers


def test_worker_can_process_messages_from_unknown_queues(stub_broker):
    results = []

    # Given that I have a broker with a default queue
    stub_broker.declare_queue("default")

    # And an actor
    @dramatiq.actor
    def do_work():
        results.append(42)

    message = do_work.message()
    message = message.copy(queue_name="unknown")

    # And I enqueue a message for an unknown queue onto the broker
    stub_broker.queues["default"].put(message.encode())

    # Given that I have a worker
    with worker(stub_broker, queues={"default"}) as stub_worker:
        # Then the worker should be able to process the message based on the actor name
        stub_worker.join()
        assert results == [42]


def test_worker_can_process_failed_messages_from_unknown_queues(stub_broker):
    # Given that I have a broker with a default queue
    stub_broker.declare_queue("default")

    # And a middleware that fails all messages
    class FailMiddleware(middleware.Middleware):
        def before_process_message(self, broker, message):
            message.fail()

    stub_broker.add_middleware(FailMiddleware())

    # And an actor
    @dramatiq.actor
    def do_work():
        pass

    some_message = do_work.message()
    some_message = some_message.copy(queue_name="unknown")

    # And I enqueue a message for an unknown queue onto the broker
    stub_broker.queues["default"].put(some_message.encode())

    # Given that I have a worker
    with worker(stub_broker, queues={"default"}) as stub_worker:
        # Then the worker should be able to process the message based on the actor name
        stub_worker.join()
        assert 1 == len(stub_broker.dead_letters_by_queue["default"])

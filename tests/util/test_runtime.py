from util.runtime import StartedProcessRegistry


class FakeProcess:
    def __init__(self, *, alive=True):
        self.alive = alive
        self.start_calls = 0
        self.terminate_calls = 0
        self.kill_calls = 0
        self.join_calls = []

    def start(self):
        self.start_calls += 1

    def is_alive(self):
        return self.alive

    def terminate(self):
        self.terminate_calls += 1
        self.alive = False

    def kill(self):
        self.kill_calls += 1
        self.alive = False

    def join(self, timeout=None):
        self.join_calls.append(timeout)


class FakeQueue:
    def __init__(self):
        self.items = []

    def put(self, item):
        self.items.append(item)


class FakeManager:
    def __init__(self):
        self.shutdown_calls = 0

    def shutdown(self):
        self.shutdown_calls += 1


def test_started_process_registry_starts_and_shuts_down_processes_in_reverse_order():
    registry = StartedProcessRegistry()
    first = FakeProcess()
    second = FakeProcess()
    queue_obj = FakeQueue()
    manager = FakeManager()

    registry.start(first, "first")
    registry.start(second, "second")
    registry.shutdown(queue_sentinels=[(queue_obj, None)], manager=manager)

    assert first.start_calls == 1
    assert second.start_calls == 1
    assert second.terminate_calls == 1
    assert first.terminate_calls == 1
    assert queue_obj.items == [None]
    assert manager.shutdown_calls == 1
    assert registry.processes == []


def test_started_process_registry_ignores_none_processes():
    registry = StartedProcessRegistry()

    result = registry.start(None, "missing")

    assert result is None
    assert registry.processes == []

"""Small unit tests for the vector-clock merge rule used by PulseNet nodes."""
import importlib.util
from pathlib import Path

NODE_FILE = Path(__file__).parent / "core" / "node.py"


def load_node_module():
    spec = importlib.util.spec_from_file_location("pulsenet_node_vector_test", NODE_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_vector_merge_takes_componentwise_maximum():
    node = load_node_module()
    node.vector_clock[:] = [2, 0, 4] + [0] * 7
    node.receive_vector([1, 3, 2])
    assert node.vector_clock == [2, 3, 4] + [0] * 7


def test_local_event_increments_only_own_component():
    node = load_node_module()
    node.vector_clock[:] = [0] * 10
    before = node.vector_clock.copy()
    event = node.log_event("Vector-clock unit test")
    expected = before.copy()
    expected[node.NODE_ID - 1] += 1
    assert event["vector_clock"] == expected
    assert node.vector_clock == expected

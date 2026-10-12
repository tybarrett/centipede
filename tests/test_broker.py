"""How the broker hands queued jobs to a limb process."""

import threading
from collections import deque

from centipede.internal import centipede_broker
from centipede.internal.centipede_broker import CentipedeBroker
from centipede.internal.limb_timing_manager import TimingManager


class FirstLimb:
    pass


class RecordingSocketHandler:
    def __init__(self):
        self.sent = []

    def send_job(self, process_id, delivery):
        self.sent.append(delivery["data_point"])


class NeverStartedTimer:
    def __init__(self, *args):
        pass

    def start(self):
        pass


def broker_with_one_process(monkeypatch):
    monkeypatch.setattr(centipede_broker.threading, "Timer", NeverStartedTimer)

    broker = CentipedeBroker.__new__(CentipedeBroker)
    broker.first_limb = FirstLimb
    broker.socket_handler = RecordingSocketHandler()
    broker.timing_manager = TimingManager()
    broker.timing_manager.init_with_limbs([FirstLimb])
    broker.timing_manager.init_new_process("process")
    broker.limb_to_queue = {"FirstLimb": deque()}
    broker.limb_to_queue_lock = {"FirstLimb": threading.Lock()}
    broker.limb_to_process_ids = {"FirstLimb": ["process"]}
    broker.process_id_is_busy = {"process": False}
    broker.process_id_busy_lock = {"process": threading.Lock()}
    return broker


def test_a_job_taken_from_the_queue_keeps_its_process_busy(monkeypatch):
    broker = broker_with_one_process(monkeypatch)
    broker.limb_to_queue["FirstLimb"].append({"data_point": "first"})

    broker.check_for_new_job()
    broker.put_data_in_pipeline("second")

    assert broker.process_id_is_busy["process"]
    assert broker.socket_handler.sent == ["first"]
    assert len(broker.limb_to_queue["FirstLimb"]) == 1

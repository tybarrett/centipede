import threading
import pickle
import multiprocessing
import uuid
import dill

from collections import deque

from centipede.internal.package import Package
from centipede.internal.broker_communicator import BrokerCommunicator, BROKER_IP, BROKER_PORT
from centipede.internal import limb_invocation_wrapper
from centipede.internal.limb_timing_manager import TimingManager

PERIODIC_JOB_CHECK_DELAY = 1

class CentipedeBroker(object):
    def __init__(self):
        multiprocessing.set_start_method("spawn")
        self.limb_to_next_limb = {}
        self.first_limb = None

        self.limb_name_to_class = {}

        self.limb_to_process_ids = {}
        self.limb_to_queue = {}
        self.limb_to_queue_lock = {}
        self.limb_to_config = {}

        self.process_id_is_busy = {}
        self.process_id_busy_lock = {}
        self.id_to_process = {}

        self.socket_handler = BrokerCommunicator()
        self.broker_server = threading.Thread(target=self.socket_handler.run_broker_server, args=(self.handle_incoming_data, ))
        self.broker_server.start()

        threading.Timer(10, self.check_for_new_job).start()

        self.timing_manager = TimingManager()

        self.support_brokers = []

        self.linked_resources = []
        self.linked_resources_lock = threading.Lock()


    def save_limb_config(self, limb, config):
        self.limb_to_config[limb.__name__] = config


    def grab_linked_resources(self):
        self.linked_resources_lock.acquire()
        resources = self.linked_resources
        self.linked_resources = []
        self.linked_resources_lock.release()
        return resources


    def set_limb_pipeline(self, limbs):
        self.first_limb = limbs[0]
        self.timing_manager.init_with_limbs(limbs)
        self.socket_handler.first_limb = limbs[0]
        for i in range(len(limbs)):
            limb_name = limbs[i].__name__

            self.limb_name_to_class[limb_name] = limbs[i]

            if i == len(limbs) - 1:
                self.limb_to_next_limb[limb_name] = None
            else:
                self.limb_to_next_limb[limb_name] = limbs[i+1].__name__

            self.limb_to_queue[limb_name] = deque([])
            self.limb_to_queue_lock[limb_name] = threading.Lock()

            self.limb_to_process_ids[limb_name] = []


    def create_process(self, limb_name):
        config = self.limb_to_config[limb_name]
        config_data = dill.dumps(config)

        process_id = str(uuid.uuid4())
        new_limb_port = self.socket_handler.get_new_port()
        # self.socket_handler.associate_port_with_process_id(new_limb_port, process_id)
        # self.socket_handler.associate_ip_with_process_id(BROKER_IP, process_id)

        # self.timing_manager.init_new_process(process_id)

        limb = self.limb_name_to_class[limb_name]
        new_process = multiprocessing.Process(target=limb_invocation_wrapper.create_limb,
                                              args=(limb, config_data, BROKER_IP, BROKER_PORT, new_limb_port))
        new_process.start()
        

    def handle_incoming_data(self, data):
        data_obj = dill.loads(data)
        if data_obj["type"] == "job_response":
            return self.handle_incoming_limb_data(data_obj)
        if data_obj["type"] == "status":
            return self.get_status_as_json()
        if data_obj["type"] == "new_support":
            return self.enroll_support_broker(data_obj)
        if data_obj["type"] == "new_process":
            return self.enroll_new_process(data_obj)


    def enroll_new_process(self, data_obj):
        class_name = data_obj["class"]
        ip = data_obj["ip"]
        port = data_obj["port"]
        process_id = data_obj["process_id"]

        self.process_id_busy_lock[process_id] = threading.Lock()
        self.process_id_busy_lock[process_id].acquire()
        self.process_id_is_busy[process_id] = False
        self.process_id_busy_lock[process_id].release()
        self.limb_to_process_ids[class_name].append(process_id)

        self.socket_handler.associate_ip_with_process_id(ip, process_id)
        self.socket_handler.associate_port_with_process_id(port, process_id)

        self.timing_manager.init_new_process(process_id)


    def enroll_support_broker(self, data_obj):
        support_ip = data_obj["ip"]
        support_port = data_obj["port"]

        self.support_brokers.append((support_ip, support_port))

        resp_obj = {}
        resp_obj["type"] = "class_list"
        resp_obj["broker_ip"] = BROKER_IP
        resp_obj["broker_port"] = BROKER_PORT
        resp_obj["configs"] = {}
        resp_obj["classes"] = []
        curr_limb = self.first_limb.__name__
        while curr_limb:
            resp_obj["classes"].append(self.limb_name_to_class[curr_limb])
            resp_obj["configs"][curr_limb] = self.limb_to_config[curr_limb]
            curr_limb = self.limb_to_next_limb[curr_limb]

        return dill.dumps(resp_obj)


    def get_status_as_json(self):
        status_data = []

        generator_dict = {}
        generator_dict["title"] = "Resource Generator"
        generator_dict["Processing Rate"] = self.timing_manager.get_limb_processing_rate()
        status_data.append(generator_dict)

        curr_limb = self.first_limb.__name__
        while curr_limb:
            limb_status_dict = {}
            limb_status_dict["title"] = curr_limb
            limb_status_dict["Processing Rate"] = self.timing_manager.get_limb_processing_rate(curr_limb)
            limb_status_dict["Queue Size"] = len(self.limb_to_queue[curr_limb])

            processes_data = []
            for process_id in self.limb_to_process_ids[curr_limb]:
                process_data = {}
                process_data["Process ID"] = process_id
                self.process_id_busy_lock[process_id].acquire()
                process_data["Status"] = self.process_id_is_busy[process_id]
                self.process_id_busy_lock[process_id].release()
                process_data["Processing Rate"] = self.timing_manager.get_process_processing_rate(process_id)
                processes_data.append(process_data)
            limb_status_dict["Processes"] = processes_data

            status_data.append(limb_status_dict)

            curr_limb = self.limb_to_next_limb[curr_limb]

        return pickle.dumps(status_data)


    def handle_incoming_limb_data(self, data_obj):
        limb_name = data_obj["limb_name"]
        first_process_id = data_obj["process_id"]
        self.timing_manager.record_process_output(first_process_id)

        # If there is a next limb, put the data in the queue for the next limb
        next_limb_name = self.limb_to_next_limb[limb_name]
        if next_limb_name:

            next_limb_is_slow = self.timing_manager.is_limb_slow(limb_name, next_limb_name)
            if len(self.limb_to_queue[next_limb_name]) > 3:
                next_limb = self.limb_name_to_class[next_limb_name]
                self.create_process(next_limb_name)
                self.timing_manager.reset_timing_info(next_limb)

            free_process = None
            for process in self.limb_to_process_ids[next_limb_name]:
                self.process_id_busy_lock[process].acquire()
                if not self.process_id_is_busy[process]:
                    free_process = process
                    self.process_id_busy_lock[process].release()
                    break
                self.process_id_busy_lock[process].release()

            if free_process:

                new_data = data_obj.copy()
                new_data["limb_name"] = next_limb_name
                new_data["process_id"] = free_process
                new_data["type"] = "job"

                self.process_id_busy_lock[free_process].acquire()
                self.process_id_is_busy[free_process] = True
                self.process_id_busy_lock[free_process].release()

                self.timing_manager.record_process_input(free_process)
                self.timing_manager.record_limb_input(next_limb_name)
                self.socket_handler.send_job(free_process, new_data)
            else:
                self.limb_to_queue_lock[next_limb_name].acquire()
                self.limb_to_queue[next_limb_name].append(data_obj)
                self.limb_to_queue_lock[next_limb_name].release()

        else: # There is no next limb for this data package; we are at the end of the pipeline
            package = data_obj["package_data"]
            self.linked_resources_lock.acquire()
            self.linked_resources.extend(package.get_linked_resources())
            self.linked_resources_lock.release()

        # Send another job to that limb if there is one in the queue
        if self.limb_to_queue[limb_name]:
            self.limb_to_queue_lock[limb_name].acquire()
            delivery = self.limb_to_queue[limb_name].popleft()
            self.limb_to_queue_lock[limb_name].release()

            delivery["limb_name"] = data_obj["limb_name"]
            delivery["process_id"] = data_obj["process_id"]
            delivery["type"] = "job"

            self.process_id_busy_lock[data_obj["process_id"]].acquire()
            self.process_id_is_busy[data_obj["process_id"]] = True
            self.process_id_busy_lock[data_obj["process_id"]].release()

            self.timing_manager.record_limb_input(limb_name)
            self.timing_manager.record_process_input(data_obj["process_id"])
            self.socket_handler.send_job(data_obj["process_id"], delivery)

        else:
            self.process_id_busy_lock[data_obj["process_id"]].acquire()
            self.process_id_is_busy[data_obj["process_id"]] = False
            self.process_id_busy_lock[data_obj["process_id"]].release()


    def put_data_in_pipeline(self, data_point):

        first_limb_name = self.first_limb.__name__

        self.timing_manager.record_incoming_job()

        new_package = Package()
        delivery = {}
        delivery["package_data"] = new_package
        delivery["data_point"] = data_point
        delivery["limb_name"] = first_limb_name
        delivery["type"] = "job"

        first_limb_is_slow = self.timing_manager.is_limb_slow(None, first_limb_name)
        if len(self.limb_to_queue[first_limb_name]) > 3:
            self.create_process(first_limb_name)
            self.timing_manager.reset_timing_info(self.first_limb)

        free_process = None
        for process_id in self.limb_to_process_ids[first_limb_name]:
            self.process_id_busy_lock[process_id].acquire()
            if not self.process_id_is_busy[process_id]:
                free_process = process_id
                self.process_id_busy_lock[process_id].release()
                break
            self.process_id_busy_lock[process_id].release()

        if free_process:
            self.process_id_busy_lock[free_process].acquire()
            self.process_id_is_busy[free_process] = True
            self.process_id_busy_lock[free_process].release()

            delivery["process_id"] = free_process
            self.timing_manager.record_process_input(free_process)
            self.timing_manager.record_limb_input(first_limb_name)
            self.socket_handler.send_job(free_process, delivery)
        else:
            self.limb_to_queue_lock[first_limb_name].acquire()
            self.limb_to_queue[first_limb_name].append(delivery)
            self.limb_to_queue_lock[first_limb_name].release()


    def check_for_new_job(self):
        first_limb_name = self.first_limb.__name__
        free_process = None
        for process_id in self.limb_to_process_ids[first_limb_name]:
            self.process_id_busy_lock[process_id].acquire()
            if not self.process_id_is_busy[process_id]:
                free_process = process_id
                self.process_id_busy_lock[process_id].release()
                break
            self.process_id_busy_lock[process_id].release()

        if free_process:
            new_delivery = None
            self.limb_to_queue_lock[first_limb_name].acquire()
            if len(self.limb_to_queue[first_limb_name]):
                new_delivery = self.limb_to_queue[first_limb_name].popleft()
            self.limb_to_queue_lock[first_limb_name].release()

            if new_delivery:
                self.process_id_busy_lock[free_process].acquire()
                self.process_id_is_busy[free_process] = True
                self.process_id_busy_lock[free_process].release()

                new_delivery["process_id"] = free_process
                self.timing_manager.record_process_input(free_process)
                self.timing_manager.record_limb_input(first_limb_name)
                self.socket_handler.send_job(free_process, new_delivery)

        threading.Timer(PERIODIC_JOB_CHECK_DELAY, self.check_for_new_job).start()

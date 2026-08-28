import socket
import dill as pickle
import threading
import dill
import uuid
import time

from centipede.internal import centipede_logger
from centipede.internal.ip_address import ip as LIMB_IP

# How long the limb thread blocks when its queue is empty before looking again.
# A bounded wait rather than an unbounded one so a missed signal costs one
# interval of latency instead of wedging the limb forever.
IDLE_WAIT_SECONDS = 1.0


class LimbInvoker(object):
    def __init__(self, limb_class, config, broker_ip, broker_port, limb_port):
        self.incoming_data_point = None
        self.incoming_package = None
        self.outgoing_data_client = None

        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.limb_class = limb_class
        self.config = config
        self.limb_port = limb_port

        self.ingest_data_lock = threading.Lock()
        # Set by the ingestion server when it hands this invoker a job, waited
        # on by the limb thread. Without it that thread has nothing to block on
        # and spins for as long as the pipeline is idle.
        self.data_available = threading.Event()
        
        self.server_running = False
        self.ingestion_server_thread = threading.Thread(target=self.run_ingestion_server, args=(limb_port, ))
        self.ingestion_server_thread.start()

        self.limb_thread = threading.Thread(target=self.run_ingestion_process, args=(limb_class, config, broker_ip, broker_port))
        self.limb_thread.start()

        self.process_id = None

    def run_ingestion_process(self, limb_class, in_config, broker_ip, broker_port):
        limb_obj = limb_class(in_config)

        while True:

            data_point = None
            package = None

            self.ingest_data_lock.acquire()
            if self.incoming_data_point and self.incoming_package:
                data_point = self.incoming_data_point
                package = self.incoming_package
                self.incoming_package = None
                self.incoming_data_point = None
            self.ingest_data_lock.release()

            if data_point and package:
                limb_obj.scrape_from_url(data_point, package)

                delivery = {}
                delivery["package_data"] = package
                delivery["data_point"] = data_point
                delivery["limb_name"] = limb_class.__name__
                delivery["process_id"] = self.process_id
                delivery["type"] = "job_response"
                pickled_package = dill.dumps(delivery)
                self.outgoing_data_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.outgoing_data_client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.outgoing_data_client.connect((broker_ip, broker_port))
                self.outgoing_data_client.sendall(pickled_package)
                self.outgoing_data_client.close()
            else:
                # Nothing was handed to us. Wait to be signalled rather than
                # looping straight back round: this loop has no other exit, so
                # without a block here every limb process burns a core for as
                # long as its queue is empty -- which, for a scraper on a six
                # hour period, is nearly all of the time.
                self.data_available.wait(IDLE_WAIT_SECONDS)
                self.data_available.clear()


    def run_ingestion_server(self, limb_port):
        incoming_data_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        incoming_data_server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        incoming_data_server.bind(("", limb_port))

        incoming_data_server.listen()
        self.server_running = True

        while True:
            conn, addr = incoming_data_server.accept()
            data = None
            try:
                data = conn.recv(16384)
            except ConnectionResetError as e:
                conn.close()
                incoming_data_server.close()
                incoming_data_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                incoming_data_server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                host = socket.gethostbyname()
                incoming_data_server.bind(("", limb_port))
                incoming_data_server.listen()
                continue
            except:
                print("Some unknown error occurred.")

            if data:
                inc_object = pickle.loads(data)

                if not self.process_id:
                    self.process_id = inc_object["process_id"]

                if inc_object["type"] == "job":
                    if not self.incoming_data_point and not self.incoming_package:

                        new_package = inc_object["package_data"]
                        self.ingest_data_lock.acquire()
                        self.incoming_package = new_package
                        self.incoming_data_point = inc_object["data_point"]
                        self.ingest_data_lock.release()
                        self.data_available.set()

                elif inc_object["type"] == "is_working":
                    pass
                    # if not self.incoming_data_point and not self.incoming_package:
                    #     conn.sendall(b"available")
                    # else:
                    #     conn.sendall(b"working")

                # conn.close()
                # incoming_data_server.close()
                # incoming_data_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # incoming_data_server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                # incoming_data_server.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE, 'eth0'.encode("utf-8"))
                # incoming_data_server.bind((LIMB_IP, limb_port))
                # incoming_data_server.bind(("192.168.1.219", limb_port))
                # incoming_data_server.listen()
                # conn, addr = incoming_data_server.accept()
            conn.close()
            # conn, addr = incoming_data_server.accept()

            # Some more clauses here, like timing stuff? idk


    def send_enrolled_signal(self):
        delivery = {}
        delivery["type"] = "new_process"
        delivery["class"] = self.limb_class.__name__
        delivery["ip"] = LIMB_IP
        delivery["port"] = self.limb_port
        delivery["process_id"] = str(uuid.uuid4())

        while not self.server_running:
            time.sleep(0.1)
            continue

        outgoing_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        outgoing_client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        outgoing_client.connect((self.broker_ip, self.broker_port))
        outgoing_client.sendall(dill.dumps(delivery))
        outgoing_client.close()


def create_limb(limb_class, config_data, broker_ip, broker_port, limb_port):

    in_config = pickle.loads(config_data)
    in_config["logger"] = centipede_logger.create_logger(str(limb_class.__name__), in_config["log_level"])

    invoker = LimbInvoker(limb_class, in_config, broker_ip, broker_port, limb_port)
    invoker.send_enrolled_signal()


if __name__ == "__main__":
    from centipede.limbs.empty_limb import EmptyLimb
    create_limb(EmptyLimb, {"log_level": logging.DEBUG}, "127.0.0.1", 10000, 10001)

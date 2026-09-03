from asciimatics.screen import Screen
from asciimatics.scene import Scene
from asciimatics.effects import Cycle, Stars
from asciimatics.renderers import FigletText

import time
import socket
import pickle
import decimal

from internal import wire
from internal.ip_address import ip as BROKER_IP

PORT = 10000

class CentipedeMonitor():
    def __init__(self):
        pass

    def fetch_data(self):
        # TODO make a request to centipede for the information
        delivery = {}
        delivery["type"] = "status"

        outgoing_data_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        outgoing_data_client.connect((BROKER_IP, PORT))
        wire.send_message(outgoing_data_client, pickle.dumps(delivery))
        data = wire.recv_message(outgoing_data_client)
        outgoing_data_client.close()

        return data

    def visualize_data(self, data, screen):

        screen.clear()
        line_counter = 0
        for limb_dict in data:

            if limb_dict["Processing Rate"]:
                rate = decimal.Decimal(limb_dict["Processing Rate"])
                screen.print_at(limb_dict["title"] + "  -  ingesting a job every " + str(round(rate, 3)) + " s", 0, line_counter)
            else:
                screen.print_at(limb_dict["title"] + "  -  ", 0, line_counter)

            line_counter += 1

            if "Queue Size" in limb_dict.keys():
                screen.print_at(str(limb_dict["Queue Size"]) + " jobs in queue.", 2, line_counter)
                line_counter += 1

            if "Processes" in limb_dict.keys():
                for process in limb_dict["Processes"]:

                    if not process["Status"]:
                        screen.print_at("•", 2, line_counter, Screen.COLOUR_GREEN)
                    else:
                        screen.print_at("•", 2, line_counter, Screen.COLOUR_RED)

                    if not process["Processing Rate"]:
                        screen.print_at(process["Process ID"] + "  -  ", 4, line_counter)
                    else:
                        rate = decimal.Decimal(process["Processing Rate"])
                        screen.print_at(process["Process ID"] + "  -  " + str(round(rate, 3)) + " s per job", 4, line_counter)

                    line_counter += 1

            line_counter += 1
        screen.refresh()


    def work(self, screen):
        while True:
            data = self.fetch_data()
            self.visualize_data(pickle.loads(data), screen)

            time.sleep(2)


def main():
    monitor_obj = CentipedeMonitor()
    Screen.wrapper(monitor_obj.work)

main()

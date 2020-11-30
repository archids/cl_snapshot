### Distributed Systems Lab 04, 2020 - Chandy Lamport Algorithm ###
### Snapshot Script ###
### by arc.hids ###

import socket
import threading
import multiprocessing
import time
import random
import pickle
from prettytable import PrettyTable

# List of all machines taking part in the snapshotting
CLIENT_LIST = {
    "10.142.0.6": {"Name": "P1", "Port": 2021},
    "10.142.0.7": {"Name": "P2", "Port": 2022},
    "10.142.0.8": {"Name": "P3", "Port": 2023}
}

# Create process IDs based on the number of processes involved in the algortithm.
NUM_PROCESSES = len(CLIENT_LIST)
PROCESS_IDS = ["P"+str(num_process)
               for num_process in range(1, NUM_PROCESSES+1)]

# Local machine information
LOCAL_ID = CLIENT_LIST[socket.gethostbyname(socket.gethostname())]["Name"]
LOCAL_IP = socket.gethostbyname(socket.gethostname())

# Local ip/port to be used as Server for listening
LOCAL_PORT = CLIENT_LIST[socket.gethostbyname(socket.gethostname())]["Port"]
HOST = socket.gethostbyname(socket.gethostname())

# Headers to distinguish between messages and markers
MARKER_HEADER = {"header": "MARKER"}
MESSAGE_HEADER = {"header": "MESSAGE"}

# Constantly updated current state of the process
current_state = {}
# Marker increment
prior_marker = False
# Local Snapshot status and stored snapshot
global_snapshot = {}
saved_snapshots = []

# Gather connected clients info for vetting
closed_channel = []


# Generate random event on a process from list of events and
# queued to the process for FIFO requirement
def generate_event():
    # Threading lock to be passed on to all events
    lock = threading.Lock()
    # Random weighted selection of an event from the event list
    if LOCAL_ID == "P1":
        EVENTS_LIST = [local_event, bcast_message, init_snapshot]
        random_event = random.choices(EVENTS_LIST, [40, 40, 20], k=1)
        # Call the randomly selected event function
        for event in random_event:
            event(lock)
    else:
        EVENTS_LIST = [local_event, bcast_message]
        random_event = random.choices(EVENTS_LIST, [70, 30], k=1)
        # Call the randomly selected event function
        for event in random_event:
            event(lock)


# Function to start an internal event that is not broadcast
def local_event(lock):
    with lock:
        name = "Local Event"
        print(f"[{LOCAL_ID}] Initiating Local Event")
        # Update the current state, increment by 1
        update_current_state()


# Function to BROADCAST MESSAGES to other process
def bcast_message(lock):
    with lock:
        name = "Message Broadcast"
    # Broadcast message out to all outgoing
    handle_outgoing(name)


# Function to BROADCAST MARKERS to other process
def bcast_marker():
    global global_snapshot
    name = "Marker Broadcast"
    # Now that we have taken local snapshot let's send markers out
    handle_outgoing(name)


# Function to INITIATE SNAPSHOT
def init_snapshot(lock):
    global saved_snapshots
    name = "Initiate Snapshot"
    with lock:
        print("\033[1m" +
              f"\t>>> [{LOCAL_ID}] INITIATING SNAPSHOT\n" + "\033[00m")
        global_snapshot[LOCAL_ID] = "Yes"
        status_report("snapshot")
        saved_snapshots.append({"Snapshot": current_state,
                                "Snapshot Time": time.time()})
    # After snapshotting send markers to other processes
    bcast_marker()


# Function to handle outgoing markers or messages
def handle_outgoing(event_name):

    try:
        for (client, info) in CLIENT_LIST.items():
            if client != LOCAL_IP:

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as outgoing_conn:
                    print(
                        f"[{LOCAL_ID}] Connecting to {info['Name']} on Port {info['Port']}")
                    outgoing_conn.connect((client, info["Port"]))

                    if event_name == "Marker Broadcast":
                        with_header = [MARKER_HEADER,
                                       current_state, global_snapshot]
                        marker = pickle.dumps(with_header)
                        print(
                            f"[{LOCAL_ID}] Broadcasting MARKER to {info['Name']}")
                        update_current_state()
                        outgoing_conn.sendall(marker)

                    elif event_name == "Message Broadcast":
                        with_header = [MESSAGE_HEADER, current_state]
                        message = pickle.dumps(with_header)
                        print(
                            f"[{LOCAL_ID}] Broadcasting MESSAGE to {info['Name']}")
                        update_current_state()
                        outgoing_conn.sendall(message)

            time.sleep(5)

    except ConnectionError as msg:
        print(msg)


# Function to incoming markers or messages
def handle_incoming():
    # Initiate Server socket
    incoming_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    incoming_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    port = LOCAL_PORT
    incoming_conn.bind((HOST, port))
    incoming_conn.listen()
    print(f"[{LOCAL_ID}] LISTENING ON PORT {port}\n")

    # Collect all incoming connections
    incoming_threads = []
    while True:
        try:
            client_socket, address = incoming_conn.accept()

            newthread = threading.Thread(
                target=handle_clients, args=(client_socket, address))
            newthread.daemon = True
            newthread.start()
            incoming_threads.append(newthread)

        except:
            print(f"[{LOCAL_ID}] Error accepting connection")

    for connection in incoming_threads:
        connection.join()

    incoming_conn.close()


# Handle incoming clients
def handle_clients(client_socket, address):
    print(f"[{LOCAL_ID}] {CLIENT_LIST[address[0]]['Name']} connected")

    try:
        data = client_socket.recv(2056)
    except EOFError as msg:
        print(f"[{LOCAL_ID}] Error receiving data")

    received_data = pickle.loads(data)
    process_data(client_socket, address, received_data)


# Process received data to update current state with global states
def process_data(client_socket, address, received_data):
    global prior_marker
    global global_snapshot
    global saved_snapshots
    global closed_channel

    lock = threading.Lock()
    try:
        # Check if there has been any prior marker received
        if not prior_marker:
            # Check the headers for incoming to see if it's a marker or message
            # If it's a message, check existing marker status and vet the channel
            if received_data[0]['header'] == "MESSAGE":
                if address[0] in closed_channel:
                    client_socket.close()
                else:
                    print(
                        f"[{LOCAL_ID}] Received {received_data[0]['header']} from {CLIENT_LIST[address[0]]['Name']}")
                    with lock:
                        for (process, state) in received_data[1].items():
                            if process != LOCAL_ID:
                                if state > current_state[process]:
                                    current_state[process] = state
                        update_current_state()
            # If it's a marker, check the marker status and initiate marker
            # receiving protocols
            elif received_data[0]['header'] == "MARKER":
                print(
                    f"[{LOCAL_ID}] Received a \033[36m{received_data[0]['header']}\033[00m from {CLIENT_LIST[address[0]]['Name']}")

                #received_snapshots = received_data[2]
                print(f"[{LOCAL_ID}] Taking local snapshot")
                global_snapshot[LOCAL_ID] = "Yes"
                saved_snapshots.append({"Snapshot": current_state,
                                        "Snapshot Time": time.time()})

                global_snapshot[CLIENT_LIST[address[0]]["Name"]] = "Yes"

                status_report("marker")
                print(f"[{LOCAL_ID}] Setting status of prior markers")
                prior_marker = True
                print(f"[{LOCAL_ID}] Added current connection to closed channel")
                closed_channel.append(address[0])
                print(f"[{LOCAL_ID}] Still looking for Global Snapshot")
                bcast_marker()

        else:
            if received_data[0]['header'] == "MARKER":
                if address[0] in closed_channel:
                    client_socket.close()
                else:
                    print(
                        f"[{LOCAL_ID}] Received \033[36m{received_data[0]['header']}\033[00m from {CLIENT_LIST[address[0]]['Name']}")

                    closed_channel.append(address[0])
                    global_snapshot[CLIENT_LIST[address[0]]["Name"]] = "Yes"
                    # received_snapshots = received_data[2]
                    if all(status == "Yes" for (snapshot, status) in global_snapshot.items()):
                        print(
                            "\033[1;31m" + "\n\t[SUCCESS] GLOBAL SNAPSHOT CAPTURED!!!\n" + "\033[00m")
                    status_report("success")
                    post_capture_cleanup()
                    time.sleep(5)

    except Exception as msg:
        print(msg)


# Function to increment local state
def update_current_state():
    global current_state
    current_state[LOCAL_ID] += 1
    status_report("state")


# Setting few variables back to default for the next Snapshot
def post_capture_cleanup():
    prior_marker = False
    closed_channel.clear()
    global_snapshot = default_global_snapshot


# Print some pretty tables of running status
def status_report(event_name):
    table = PrettyTable()

    if event_name == "state":
        table.add_column('Process', [process for process in PROCESS_IDS])
        table.add_column('State', [state for state in current_state.values()])

    elif event_name == "success":
        table.title = "FINAL SNAPSHOT"
        table.add_column('Process', [process for process in PROCESS_IDS])
        table.add_column(
            'Snapshot', [status for status in global_snapshot.values()])
        table.add_column('State', [state for state in current_state.values()])

    elif event_name == "marker" or event_name == "snapshot":
        table.title = "SNAPSHOT STATE"
        table.add_column('Process', [process for process in PROCESS_IDS])
        table.add_column(
            'Snapshot', [status for status in global_snapshot.values()])

    print(table)
    print()


# Main function
def main():
    # Variables to hold and manipulate local and global states
    global current_state
    initial_state = {}

    print(f"CREATING PROCESS: {LOCAL_ID}")

    for process_id in PROCESS_IDS:
        initial_state[process_id] = 0
        global_snapshot[process_id] = "No"

    global default_global_snapshot
    default_global_snapshot = global_snapshot

    current_state = initial_state

    # print(f"(PID: {name}, Initial State: {initial_state})")

    # Spawn a worker thread for server functions to handle all
    # incoming data
    incoming_thread = threading.Thread(target=handle_incoming)
    incoming_thread.daemon = True
    incoming_thread.start()

    time.sleep(3)

    while True:
        # Simulate random events; local, outgoing messages, snapshot
        generate_event()
        time.sleep(random.randint(5, 30))


if __name__ == "__main__":
    main()

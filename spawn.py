import subprocess
import sys
import time

def spawn_clients(go_executable_path, topic, num_clients):
    """
    Spawns multiple clients by running the Go client program as subprocesses.
    
    Args:
        go_executable_path (str): Path to the compiled Go client executable.
        topic (str): Topic to subscribe to for each client.
        num_clients (int): Number of client processes to spawn.
    """
    processes = []

    try:
        print(f"Spawning {num_clients} clients subscribing to topic '{topic}'...")
        for i in range(num_clients):
            process = subprocess.Popen(
                [go_executable_path, "-topic", topic, "-pid", str(i+1)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            processes.append(process)
            print(f"Client {i + 1} started.")

        # Continuously monitor the processes
        while processes:
            for i, process in enumerate(processes):
                retcode = process.poll()  # Check if the process has terminated
                if retcode is not None:  # Process has exited
                    stdout, stderr = process.communicate()
                    print(f"Client {i + 1} exited with code {retcode}.")
                    if stdout:
                        print(f"Client {i + 1} output:\n{stdout}")
                    if stderr:
                        print(f"Client {i + 1} errors:\n{stderr}")
                    processes.remove(process)
    except KeyboardInterrupt:
        print("\nTerminating all clients...")
        for process in processes:
            process.terminate()
        sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python spawn_clients.py <go_executable_path> <topic> <num_clients>")
        sys.exit(1)

    go_executable_path = sys.argv[1]
    topic = sys.argv[2]
    num_clients = int(sys.argv[3])

    spawn_clients(go_executable_path, topic, num_clients)

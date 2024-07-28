import os
import time
import random
import argparse
import concurrent.futures

PACKAGE_ROOT = "/Users/daan/Dev/Multiprocessing/log_analysis_system"
LOG_FILE_PATH = PACKAGE_ROOT + "/log_files/"

# Generate 100 random ip address
def generate_ip_address():
    ip_address = []
    for i in range(100):
        ip_address.append(".".join([str(random.randint(0, 255)) for _ in range(4)]))
    return ip_address

# Generate 50 random users
def generate_users():
    users = []
    for i in range(50):
        users.append("user_" + str(i))
    return users

# Generate  random actions
def generate_actions():
    actions = ["GET", "POST", "DELETE", "PUT"]
    return actions

IP_ADDRESS = generate_ip_address()
USER_NAMES = generate_users()
ACTIONS = generate_actions()

def get_random_response_time_in_ms():
    return random.randint(1, 1000)

def get_randome_ip(ip_addresses):
    return random.choice(ip_addresses)

def get_random_user(users):
    return random.choice(users)

def get_random_action(actions):
    return random.choice(actions)

def get_line():
    return "::".join([
        str(int(time.time())), 
        get_randome_ip(IP_ADDRESS), 
        get_random_user(USER_NAMES),
        get_random_action(ACTIONS),
        str(get_random_response_time_in_ms())]) + "\n"

def generate_single_log_file(size_in_gb, index):
    log_file_path = LOG_FILE_PATH + "log_file_" + str(index) + ".log"
    example_line = get_line()
    example_line_size = len(example_line.encode('utf-8'))
    size_in_bytes = size_in_gb * 1024 * 1024 * 1024
    num_lines = size_in_bytes // example_line_size
    with open(log_file_path, 'a') as f:
        for _ in range(num_lines):
            line = get_line()
            f.write(line)
    return index

def main(args):
    if not os.path.exists(LOG_FILE_PATH):
        os.makedirs(LOG_FILE_PATH)
    
    n_files = args.num_files
    size_in_gb = args.size
    print(f"n_files: {n_files} size: {size_in_gb}")
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for i in range(n_files):
            f = executor.submit(generate_single_log_file, size_in_gb, i)
            futures.append(f)

    for fut in concurrent.futures.as_completed(futures):
        print(f"File {str(fut.result())} is ready")

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--size", type=int, help="Size of the log file in GB")
    args.add_argument("--num_files", type=int, help="Number of log files to generate")
    args = args.parse_args()
    main(args)
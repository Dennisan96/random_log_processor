import logging
import cProfile
import io
import pstats
import psutil

logger = logging.getLogger(__name__)


def write_output_to_file(
    file_name,
    user_count,
    n_unique_ips,
    avg_action_time,
):
    with open(file_name, 'w') as f:
        f.write("Users count: \n")
        i = 0
        for user, count in user_count.items():
            f.write(f"{user} : {count}\n")
            i += 1
            if i == 10:
                break 
            
        f.write("\n")

        f.write("Average response time per action: \n")
        for action, avg_time in avg_action_time.items():
            f.write(f"{action} : {avg_time}\n")

        f.write(f'\n N of unique ip address {n_unique_ips}\n')
        
def process_one_line(
    user_count, 
    action_count,
    action_total_time,
    unique_ips,
    line,
):
    try:
        parts = line.split('::')
        ip, user, action, response_time = parts[1], parts[2], parts[3], int(parts[4])
        action_count[action] += 1
        action_total_time[action] += response_time
        user_count[user] += 1
        unique_ips.add(ip)
    except Exception as e:
        logger.exception(f"Error processing line {line}")


def add_dictionary_to_d1(dict1, dict2):
    for key, value in dict2.items():
        if key not in dict1:
            dict1[key] = value
        else:
            dict1[key] += value
    return dict1

def add_set_to_s1(set1, set2):
    for item in set2:
        set1.add(item)
    return set1


import time

class TimeAccumulator:
    def __init__(self):
        self.total_time = 0
        self.call_count = 0

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            
            self.total_time += (end_time - start_time)
            self.call_count += 1
            
            return result
        return wrapper

    def print_stats(self):
        print(f"Function called {self.call_count} times")
        print(f"Total execution time: {self.total_time:.6f} seconds")
        if self.call_count > 0:
            print(f"Average execution time: {self.total_time / self.call_count:.6f} seconds")


def check_resource(): 
    cpu_percent = psutil.cpu_percent()
    mem_percent = psutil.virtual_memory().percent

    if cpu_percent >= 80 or mem_percent >= 70:
        print(f"UnSafe to processing one additional file")
        return False
    return True

# # Create an instance of the accumulator
# time_accumulator = TimeAccumulator()

# # Apply the decorator to your function
# @time_accumulator
# def function_to_measure(n):
#     # Your function code here
#     return sum(range(n))

# # Call the function multiple times
# for i in range(1000):
#     function_to_measure(1000)

# # Print the accumulated stats
# time_accumulator.print_stats()
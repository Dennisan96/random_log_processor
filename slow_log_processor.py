from log_generator import LOG_FILE_PATH
import os
import collections
import time
# from memory_profiler import profile
from utils import write_output_to_file, process_one_line

# @profile
def slow_mode():
    users_count = collections.defaultdict(int)
    action_count = collections.defaultdict(int)
    total_time_per_action = collections.defaultdict(int)
    unqiue_ips = set()
    for file_name in os.listdir(LOG_FILE_PATH):
        if file_name.endswith(".log"):
            with open(LOG_FILE_PATH + file_name, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    process_one_line(
                        user_count=users_count,
                        action_count=action_count,
                        action_total_time=total_time_per_action,
                        unique_ips=unqiue_ips,
                        line=line
                    )
    avg_action_time = {}
    for action, total_response_time in total_time_per_action.items():
        avg_action_time[action] = total_response_time / action_count[action]

    user_counts_ranked = {k: v for k, v in sorted(users_count.items(), key=lambda item: item[1], reverse=True)}
    avg_action_time = {k: v for k, v in sorted(avg_action_time.items(), key=lambda item: item[1], reverse=True)}

    write_output_to_file('slow_mode_output.txt', user_counts_ranked, len(unqiue_ips), avg_action_time)
        

if __name__ == "__main__":
    t0 = time.time()
    slow_mode()
    t1 = time.time()
    print(f'Slow mode took {t1 - t0} seconds')
"""
This does not work
"""

import os
import collections
import concurrent.futures
import time
import argparse
import multiprocessing
import psutil
import mmap
import re
from functools import partial

from log_generator import LOG_FILE_PATH

def process_chunk(chunk, pattern):
    action_count = collections.Counter()
    action_total_time = collections.Counter()
    unique_ips = set()
    user_count = collections.Counter()

    for line in chunk.split(b'\n'):
        if line:
            try:
                timestamp, ip, user, action, response_time = line.decode('utf-8').strip().split('::')
                unique_ips.add(ip)
                user_count[user] += 1
                action_count[action] += 1
                action_total_time[action] += int(response_time)
            except ValueError:
                # Skip malformed lines
                continue

    return action_count, action_total_time, unique_ips, user_count

def process_one_file_multi_thread(file_name, num_threads):
    abs_file_name = os.path.join(LOG_FILE_PATH, file_name)
    file_size = os.path.getsize(abs_file_name)

    with open(abs_file_name, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        chunk_size = file_size // num_threads

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(process_chunk, mm[i:i+chunk_size], None)
                for i in range(0, file_size, chunk_size)
            ]

            results = [future.result() for future in futures]

    f_action_count = sum((result[0] for result in results), collections.Counter())
    f_action_total_time = sum((result[1] for result in results), collections.Counter())
    f_unique_ips = set().union(*(result[2] for result in results))
    f_user_count = sum((result[3] for result in results), collections.Counter())

    return f_action_count, f_action_total_time, f_unique_ips, f_user_count

def process_files_based_on_concurrency(args):
    output_file_name = args.output_file
    log_files = [f for f in os.listdir(LOG_FILE_PATH) if f.endswith('.log')]
    
    f_action_count = collections.Counter()
    f_action_total_time = collections.Counter()
    f_unique_ips = set()
    f_user_count = collections.Counter()

    max_workers = min(32, os.cpu_count() + 4)
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_one_file_multi_thread, log_file, 4)
            for log_file in log_files
        ]

        for future in concurrent.futures.as_completed(futures):
            action_count, action_total_time, unique_ips, user_count = future.result()
            f_action_count.update(action_count)
            f_action_total_time.update(action_total_time)
            f_unique_ips.update(unique_ips)
            f_user_count.update(user_count)

    avg_action_time = {
        action: total_time / f_action_count[action]
        for action, total_time in f_action_total_time.items()
    }

    user_counts_ranked = dict(sorted(f_user_count.items(), key=lambda x: x[1], reverse=True))
    avg_action_time = dict(sorted(avg_action_time.items(), key=lambda x: x[1], reverse=True))

    write_output_to_file(output_file_name, user_counts_ranked, len(f_unique_ips), avg_action_time)

def write_output_to_file(output_file_name, user_counts_ranked, unique_ip_count, avg_action_time):
    with open(output_file_name, 'w') as f:
        f.write(f"Unique IP addresses: {unique_ip_count}\n\n")
        f.write("User request counts (top 10):\n")
        for user, count in list(user_counts_ranked.items())[:10]:
            f.write(f"{user}: {count}\n")
        f.write("\nAverage response times per action (top 10):\n")
        for action, avg_time in list(avg_action_time.items())[:10]:
            f.write(f"{action}: {avg_time:.2f}ms\n")

def main(args):
    t0 = time.time()
    process_files_based_on_concurrency(args)
    print(f"Finish processing in {time.time() - t0:.2f} seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_file', "-o", default='fast_output.txt', type=str, help='Output file name')
    parser.add_argument("--algo", default='simple', type=str, help='Algorithm to use')
    args = parser.parse_args()
    main(args)
"""
Fastest way: concurrent processing of multiple files with multiple threads reading chunks of the file.50 secs for 20 log files.

When using mmap way, the memory is about 18gb peaks
"""

from slow_log_processor import LOG_FILE_PATH
import collections
import concurrent.futures
import os
from utils import (
    write_output_to_file, 
    process_one_line, 
    add_dictionary_to_d1, 
    TimeAccumulator, 
    add_set_to_s1, 
    check_resource,
)
import time
import argparse
import multiprocessing
import psutil
import mmap
from functools import wraps
import cProfile
import io
import pstats

N_CHUNKS = 36
time_acc = TimeAccumulator()

def process_one_file(file_name):
    print(f"Processing file {file_name}")
    log_file = LOG_FILE_PATH + file_name
    action_count = collections.defaultdict(int)
    action_total_time = collections.defaultdict(int)
    unique_ips = set()
    user_count = collections.defaultdict(int)
    with open(log_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            process_one_line(
                user_count=user_count,
                action_count=action_count,
                action_total_time=action_total_time,
                unique_ips=unique_ips,
                line=line
            )
    return action_count, action_total_time, unique_ips, user_count

def process_chunk(file_name, start, end, action_count, action_total_time, unique_ips, user_count):
    # print(f'Start to {file_name} at FP: {start}')
    lines = []
    with open(file_name, 'r') as f:
        f.seek(start)
        size = end - start
        lines = f.read(size).split('\n')

    for line in lines:
        if not line:
            continue
        process_one_line(
            user_count=user_count,
            action_count=action_count,
            action_total_time=action_total_time,
            unique_ips=unique_ips,
            line=line
        )
    return
        
        
def process_one_file_multi_thread(file_name, thread_count=4):
    print(f"Start to process {file_name} with multiple thread, each thread read a chunck")
    """This looks like the fastest way of processsing 20 files 1G of log files. """
    ab_file_name = LOG_FILE_PATH + file_name
    file_size = os.path.getsize(ab_file_name)
    chunk_size = file_size // N_CHUNKS
    futures = []

    offsets = []
    with open(ab_file_name, 'r') as f:
        for i in range(N_CHUNKS):
            start = f.tell()
            f.seek(start + chunk_size)
            f.readline()
            end = f.tell()
            offsets.append((start, end))
    # print(f"offsets for {file_name}: {offsets}")
    # return 
    # print(f'Start to processing chunks of {file_name}')
    f_action_count = collections.defaultdict(int)
    f_action_total_time = collections.defaultdict(int)
    f_unique_ips = set()
    f_user_count = collections.defaultdict(int)
    with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
        while offsets:
            # if not check_resource():
            #     print(f'System overload, wait for 2 secs')
            #     time.sleep(2)
            #     continue

            start, end = offsets.pop()

            fut = executor.submit(
                process_chunk, 
                ab_file_name, start, end, f_action_count, f_action_total_time, f_unique_ips, f_user_count
            )
            futures.append(fut)

    concurrent.futures.wait(futures)
    
    return f_action_count, f_action_total_time, f_unique_ips, f_user_count

def process_one_file_multi_thread_mmap(file_name, thread_count=4):

    def process_chunk_with_mmap(
        mm,
        start,
        end, 
        f_action_count,
        f_action_total_time,
        f_unique_ips,
        f_user_count,
    ):
        mm.seek(start)
        chunk = mm.read(end - start)
        lines = chunk.split(b'\n')

        for line in lines:
            if line:
                line = line.decode('utf-8')
                process_one_line(
                    user_count=f_user_count,
                    action_count=f_action_count,
                    action_total_time=f_action_total_time,
                    unique_ips=f_unique_ips,
                    line=line
                )
        return

    print(f"Start to process {file_name}")
    ab_file_name = LOG_FILE_PATH + file_name

    f_action_count = collections.defaultdict(int)
    f_action_total_time = collections.defaultdict(int)
    f_unique_ips = set()
    f_user_count = collections.defaultdict(int)

    with open(ab_file_name, 'rb') as f:
        mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ)
        file_size = os.path.getsize(ab_file_name)
        chunk_size = file_size // N_CHUNKS

        offsets = []
        for i in range(N_CHUNKS):
            start = mm.tell()
            mm.seek(min(start + chunk_size, file_size))
            mm.readline()
            end = mm.tell()
            offsets.append((start, end))

        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            while offsets:
                # if not check_resource():
                #     print(f'System overload, wait for 2 secs')
                #     time.sleep(2)
                #     continue 

                start, end = offsets.pop()

                future = executor.submit(
                    process_chunk_with_mmap, 
                    mm,
                    start, 
                    end,
                    f_action_count,
                    f_action_total_time,
                    f_unique_ips,
                    f_user_count
                )
                futures.append(future)
        
        concurrent.futures.wait(futures)
    return f_action_count, f_action_total_time, f_unique_ips, f_user_count 

# def process_one_file_multi_process(args):
#     output_file_name = args.output_file
#     algo = args.algo
#     if algo == 'simple':
#         f = process_one_file
#     else:
#         f = process_one_file_multi_thread

#     log_files = os.listdir(LOG_FILE_PATH)
#     futures = []
#     with concurrent.futures.ProcessPoolExecutor() as executor:
#         for file in log_files:
#             if file.endswith('.log'):
#                 future = executor.submit(f, file)
#             futures.append(future)
    
#     f_action_count = {}
#     f_action_total_time = {}
#     f_unique_ips = set()
#     f_user_count = {}
#     for fut in concurrent.futures.as_completed(futures):
#         action_count, action_total_time, unique_ips, user_count = fut.result()
#         add_dictionary_to_d1(f_action_count, action_count)
#         add_dictionary_to_d1(f_action_total_time, action_total_time)
#         add_dictionary_to_d1(f_user_count, user_count)
#         add_set_to_s1(f_unique_ips, unique_ips)
    
#     avg_action_time = {}
#     for action, total_response_time in f_action_total_time.items():
#         avg_action_time[action] = total_response_time / f_action_count[action]
    
#     user_counts_ranked = {k: v for k, v in sorted(f_user_count.items(), key=lambda item: item[1], reverse=True)}
#     avg_action_time = {k: v for k, v in sorted(avg_action_time.items(), key=lambda item: item[1], reverse=True)}

#     write_output_to_file(output_file_name, user_counts_ranked, len(f_unique_ips), avg_action_time)


def process_files_based_on_concurrency(args):
    
    def handle_futures(
        active_futures, 
        f_action_count, 
        f_action_total_time, 
        f_unique_ips, 
        f_user_count
    ):
        done, active_futures = concurrent.futures.wait(
            active_futures, 
            timeout=5, 
            return_when=concurrent.futures.FIRST_COMPLETED
        )
        for fut in done:
            action_count, action_total_time, unique_ips, user_count = fut.result()
            add_dictionary_to_d1(f_action_count, action_count)
            add_dictionary_to_d1(f_action_total_time, action_total_time)
            add_dictionary_to_d1(f_user_count, user_count)
            add_set_to_s1(f_unique_ips, unique_ips)
        return active_futures

    # mgr = DynamicConcurrencyManager()
    output_file_name = args.output_file
    log_files = os.listdir(LOG_FILE_PATH)
    f_action_count = collections.defaultdict(int)
    f_action_total_time = collections.defaultdict(int)
    f_unique_ips = set()
    f_user_count = collections.defaultdict(int)
    active_futures = set()

    if args.algo == 'mmap':
        print("Processing using mmap for i/o processing.")
        f = process_one_file_multi_thread_mmap
    else:
        f = process_one_file_multi_thread
    
    with concurrent.futures.ProcessPoolExecutor() as executor:
        while log_files or active_futures:
            while log_files:
                # time to spin up one addii
                log_file = log_files.pop()
                print(f"Processing log file {log_file}")
                future = executor.submit(f, log_file)
                active_futures.add(future)
            
            if active_futures:
                active_futures = handle_futures(
                    active_futures,
                    f_action_count,
                    f_action_total_time,
                    f_unique_ips,
                    f_user_count
                )
            else:
                time.sleep(1)

    avg_action_time = {}
    for action, total_response_time in f_action_total_time.items():
        avg_action_time[action] = total_response_time / f_action_count[action]

    user_counts_ranked = {k: v for k, v in sorted(f_user_count.items(), key=lambda item: item[1], reverse=True)}
    avg_action_time = {k: v for k, v in sorted(avg_action_time.items(), key=lambda item: item[1], reverse=True)}

    write_output_to_file(output_file_name, user_counts_ranked, len(f_unique_ips), avg_action_time)

def monitor_process(proc, threshold_mb):
    # This doesn't work as it only monitor the main process memory usage
    # TODO: monitor all subprocesses memory usage and kill if exceed threshold
    print(f"Start watching process memory usage, will kill after exceed {(threshold_mb / 1024):.2f} GB")
    while proc.is_alive():
        memory_usage = psutil.Process(proc.pid).memory_info().rss / (1024 * 1024)
        # print(f'Mem Usage: {(memory_usage / 1024):.2f} GB')
        if memory_usage > threshold_mb:
            print(f"Memory usage exceeded {(threshold_mb / 1024):.2f} GB. Terminating script.")
            proc.terminate()
            break
        time.sleep(0.5)
            
def main(args):
    mem_total = psutil.virtual_memory().total
    total_mem_mb = mem_total / (1024*1024)
    algo = args.algo
    if algo == 'simple':
        # this could also blow up memory if number of files is large enough?
        f = process_one_file_multi_process
    else:
        f = process_files_based_on_concurrency

    main_process = multiprocessing.Process(target=f, args=(args,))
    main_process.start()
    monitor_process(main_process, total_mem_mb * 0.7)
    main_process.join()

        
if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument('--output_file', "-o", default='fast_output.txt', type=str, help='Output file name')
    args.add_argument("--algo", default='simple', type=str, help='Algorithm to use')
    args = args.parse_args()
    t0 = time.time()
    main(args)
    print(f"Finish processing in {time.time() - t0} seconds")
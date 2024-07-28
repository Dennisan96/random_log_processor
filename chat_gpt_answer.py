from collections import defaultdict
import os
from log_generator import LOG_FILE_PATH

def parse_line(line):
    parts = line.strip().split('::')
    if len(parts) == 5:
        _, _, user_name, _, _ = parts
        return user_name
    return None

def process_logs(directory, file_pattern, num_files):
    user_count = defaultdict(int)
    
    # Process each file
    for i in range(num_files):
        file_path = os.path.join(directory, f'{file_pattern}_{i}.log')
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    user_name = parse_line(line)
                    if user_name:
                        user_count[user_name] += 1
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    # Sort the usernames by count and return the top 10
    top_ten_users = sorted(user_count.items(), key=lambda item: item[1], reverse=True)[:10]
    return top_ten_users

# Example usage
directory = LOG_FILE_PATH
file_pattern = 'log_file'
num_files = 10
top_users = process_logs(directory, file_pattern, num_files)
for user, count in top_users:
    print(f'{user}: {count}')
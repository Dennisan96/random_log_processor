import mmap
import os

from log_generator import LOG_FILE_PATH

files = os.listdir(LOG_FILE_PATH)
for f in files:
    if f.endswith('.log'):
        with open(os.path.join(LOG_FILE_PATH, f), 'rb') as file:
            mm = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
            print(mm.readline())
            mm.close()
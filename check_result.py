SLOW_MODE_FILE = 'fast_output_mmap.txt'
FAST_MODE_FILE =  'fast_output_no_mmap.txt'

with open(SLOW_MODE_FILE, 'r') as f1, open(FAST_MODE_FILE, 'r') as f2:
    lines1 = set(f1.readlines())
    lines2 = set(f2.readlines())

    assert lines1 == lines2, "Files are not the same"
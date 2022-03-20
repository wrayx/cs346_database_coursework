import subprocess
import os
import shlex

run_mapreduce_commands = shlex.split("hadoop jar TopKFloorSpace.jar com.cs346id18.part3.q2.TopKFloorSpace 10 2451146 2452268 input/1G/store_sales/store_sales.dat input/1G/store/store.dat output/q2")

with open('test_output_mapreduce.txt', 'w') as f:
    # poll = None
    process = subprocess.Popen(run_mapreduce_commands, stderr=f, universal_newlines=True)
    process.wait()
    f.close()

with open('test_output_mapreduce.txt', 'r') as f:
    # Loop through the file line by line
    for line in f:
        # checking string is present in line or not
        if "CPU time spent" in line:
            print(line.lstrip(), end = '')
        elif "memory (bytes) snapshot" in line:
            print(line.lstrip(), end = '')
        elif "bytes read=" in line:
            print(line.lstrip(), end = '')
        elif "bytes written=" in line:
            print(line.lstrip(), end = '')
        elif "Total time spent by all map tasks (ms)" in line:
            print(line.lstrip(), end = '')
        elif "Total time spent by all reduce tasks (ms)" in line:
            print(line.lstrip(), end = '')
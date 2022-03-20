import subprocess
import os
import shlex

rm_commands = shlex.split("./run.sh 2 rm")
# makejar_commands = shlex.split("jar -cf TopKFloorSpace.jar com/cs346id18/part3/q2/TopKFloorSpace*.class")
run_mapreduce_commands = shlex.split("hadoop jar TopKFloorSpace.jar com.cs346id18.part3.q2.TopKFloorSpace 10 2451146 2452268 input/1G/store_sales/store_sales.dat input/1G/store/store.dat output/q2")
# process = subprocess.call(rm_commands, stdout=subprocess.PIPE, universal_newlines=True)
# process2 = subprocess.Popen(makejar_commands, shell=True, stdout=subprocess.PIPE, universal_newlines=True)
# process2.communicate()
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
        elif "memory" in line:
            print(line.lstrip(), end = '')
        elif "Bytes Read" in line:
            print(line.lstrip(), end = '')
        elif "Bytes Written" in line:
            print(line.lstrip(), end = '')
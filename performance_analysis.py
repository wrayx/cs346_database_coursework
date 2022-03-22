import subprocess
import sys
import shlex

if sys.argv[1] == 'a':
    run_mapreduce_commands = shlex.split("hadoop jar TopKNetProfit.jar com.cs346id18.part3.q1a.TopKNetProfit 10 2451146 2452268 input/40G/store_sales/store_sales.dat output/q1a")
elif sys.argv[1] == 'b':
    run_mapreduce_commands = shlex.split("hadoop jar TopKItems.jar com.cs346id18.part3.q1b.TopKItems 5 2451146 2452268 input/40G/store_sales/store_sales.dat output/q1b")
elif sys.argv[1] == 'c':
    run_mapreduce_commands = shlex.split("hadoop jar TopKDays.jar com.cs346id18.part3.q1c.TopKDays 10 2451392 2451894 input/40G/store_sales/store_sales.dat output/q1c")
else:
    run_mapreduce_commands = shlex.split("hadoop jar TopKFloorSpace.jar com.cs346id18.part3.q2.TopKFloorSpace 10 2451146 2452268 input/40G/store_sales/store_sales.dat input/40G/store/store.dat output/q2")

with open('test_output_mapreduce.txt', 'w') as f:
    # poll = None
    # process0 = subprocess.Popen(, stderr=f, universal_newlines=True)
    process = subprocess.Popen(run_mapreduce_commands, stderr=f, universal_newlines=True)
    process.wait()
    f.close()

output = ["", "", "", "", "", ""]

with open('test_output_mapreduce.txt', 'r') as f:

    # Loop through the file line by line
    for line in f:
        # checking string is present in line or not
        if "CPU time spent" in line:
            time = int(line.lstrip().split('=').pop())/1000
            output[0] = "CPU time spent (secs)={t}".format(t=time)
        elif "Total time spent by all map tasks (ms)" in line:
            time = int(line.lstrip().split('=').pop())/1000
            output[1] = "map tasks (secs)={t}".format(t=time)
        elif "Total time spent by all reduce tasks (ms)" in line:
            time = int(line.lstrip().split('=').pop())/1000
            output[2] = "reduce tasks (secs)={t}".format(t=time)
        elif "memory (bytes) snapshot" in line:
            # print(line.lstrip(), end = '')
            output[3] = line.strip()
        elif "bytes read=" in line:
            # print(line.lstrip(), end = '')
            output[4] = line.strip()
        elif "bytes written=" in line:
            # print(line.lstrip(), end = '')
            output[5] = line.strip()
output_str = "\n{str}\n".format(str='\n'.join(output))
print(output_str)

with open('performance_summary.txt', 'a') as f:
    f.write("\ntask_{}".format(sys.argv[1]))
    f.write(output_str)
    f.close()


from audioop import avg
from genericpath import isdir
import os 
from os.path import isfile, join
from os import abort, listdir
from statistics import mean
from statistics import stdev


core_run_op = 1000

dir_path = os.path.dirname(os.path.realpath(__file__))

print(os.path.basename(__file__), dir_path)

files = [f for f in listdir(dir_path) if f != os.path.basename(__file__)]

print(files)

times_files = []
conflicts_files = []
blocks_files = []

for file in files:
    file_path = dir_path + "\\" + file
    file1 = open(file_path, 'r')
    lines = file1.readlines()

    # print(lines[2].strip())

    times = []
    conflicts = []
    blocks = []

    time_t = []
    conflict_t = []
    block_t = []
    for line in lines:
        if "(" in line:
            parse = line.strip().split()
            time_t.append(float(parse[-2]))
            block_t.append(float(parse[-1]))
            conflict_t.append(float(parse[-4]))
        else:
            if time_t.__len__() != 0:
                time = max(time_t)
                conflict = sum(conflict_t) / 1000
                block = sum(block_t) / sum(time_t)

                times.append(time)
                conflicts.append(conflict)
                blocks.append(block)

                time_t = []
                conflict_t = []
                block_t = []
    times_files.append(times)
    conflicts_files.append(conflicts)
    blocks_files.append(blocks)

file_cnt = files.__len__()
queue_size_cnt = times_files[0].__len__()

# print(queue_size_cnt)
print("delay_cnt\ttps_avg\ttps_dev\tconflict_rate_avg\tconflict_rate_dev\tblock_rate_avg\tblock_rate_dev")
for i in range(queue_size_cnt):
    tps_list = []
    conflict_list = []
    block_list = []
    for j in range(file_cnt):
        tps_list.append(core_run_op / times_files[j][i])
        conflict_list.append(conflicts_files[j][i])
        block_list.append(blocks_files[j][i])

    print(i+1, mean(tps_list), stdev(tps_list), mean(conflict_list), stdev(conflict_list), mean(block_list), stdev(block_list))
    



            

    



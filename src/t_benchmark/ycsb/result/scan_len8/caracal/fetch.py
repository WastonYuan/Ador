

from audioop import avg
from genericpath import isdir
import os 
from os.path import isfile, join
from os import abort, listdir
from statistics import mean
from statistics import stdev

core_cnt = 16

core_run_op = 1000

dir_path = os.path.dirname(os.path.realpath(__file__))

print(os.path.basename(__file__), dir_path)

files = [f for f in listdir(dir_path) if f != os.path.basename(__file__)]

print(files)

thetas = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
thd_cnt = 16

init_step = float(4000) / float(thd_cnt)

times_files = []
blocks_files = []

for file in files:
    file_path = dir_path + "\\" + file
    file1 = open(file_path, 'r')
    lines = file1.readlines()

    # print(lines[2].strip())

    times = []
    blocks = []

    time_t = []
    block_t = []
    for line in lines:
        if "read_cnt" not in line:
            parse = line.strip().split()
            time_t.append(float(parse[-2]) + init_step)
            block_t.append(float(parse[-1]))
        else:
            if time_t.__len__() != 0:
                time = max(time_t)
                block = sum(block_t) / sum(time_t)

                times.append(time)
                blocks.append(block)

                time_t = []
                conflict_t = []
                block_t = []
    if time_t.__len__() != 0:
        time = max(time_t)
        block = sum(block_t) / sum(time_t)
        times.append(time)
        blocks.append(block)
        time_t = []
        conflict_t = []
        block_t = []

    times_files.append(times)
    blocks_files.append(blocks)

file_cnt = files.__len__()
theta_cnt = times_files[0].__len__()

# print(queue_size_cnt)
print("theta\ttime_avg\ttime_dev\tblock_rate_avg\tblock_rate_dev")
for i in range(theta_cnt):
    tps_list = []
    block_list = []
    for j in range(file_cnt):
        if core_cnt < thd_cnt:
            tps_list.append(core_run_op / times_files[j][i] * core_cnt / thd_cnt)
        else:
            tps_list.append(core_run_op / times_files[j][i])
        block_list.append(blocks_files[j][i])

    print(thetas[i], mean(tps_list), stdev(tps_list), mean(block_list), stdev(block_list))
    



            

    



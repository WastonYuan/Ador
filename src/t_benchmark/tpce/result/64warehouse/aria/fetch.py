

from genericpath import isdir
import os 
from os.path import isfile, join
from os import abort, listdir



# Aria each core run how much cnt (M) per sec
# 0:read_cnt 1:write_cnt 2:commit_rw_step_cnt 3:commit_scan_step_cnt 4:commit_cnt
time_costs = [1.0, 1.0, 0, 0, 0]

core_cnt = 16

dir_path = os.path.dirname(os.path.realpath(__file__))

print(os.path.basename(__file__), dir_path)

files = [f for f in listdir(dir_path) if f != os.path.basename(__file__)]

print(files)

for file in files:
    file_path = dir_path + "\\" + file
    file1 = open(file_path, 'r')
    lines = file1.readlines()

    all_tps = []
    batches_tps = [] # (thread, )

    threads_cost = []
    batches_cost = []
    batch_cnt = 1
    test_cnt = 1
    for i, line in enumerate(lines, start=0):
        
        if "batch" in line or "read_cnt" in line:
            if  threads_cost.__len__() != 0:
                # print(threads_cost)
                batch_cost = max(threads_cost)
                if len(threads_cost) > core_cnt:
                    batch_cost = batch_cost * len(threads_cost) / core_cnt
                batches_cost.append(batch_cost)
                threads_cost = []
                # print(str(batch_cnt) + " batch_cost: " + str(batch_cost))
                batch_cnt = batch_cnt + 1
            if "read_cnt" in line:
                if batches_cost.__len__() != 0:
                    print(str(test_cnt) + " tps: " + str(sum(batches_cost)))
                    test_cnt = test_cnt + 1
                    batches_cost = []
                    batch_cnt = 1
        else:
            thread_cost = 0
            thread_op_list = [i.strip() for i in line.split()]
            for i in range(len(time_costs)):
                thread_cost = thread_cost + time_costs[i] * float(thread_op_list[i])
            threads_cost.append(thread_cost)
    print(str(test_cnt) + " tps: " + str(sum(batches_cost)))
            
        




            

    


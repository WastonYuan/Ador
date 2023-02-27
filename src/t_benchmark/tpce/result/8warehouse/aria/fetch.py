

from genericpath import isdir
import os 
from os.path import isfile, join
from os import abort, listdir
from statistics import mean
from statistics import stdev

core_run_op = 1000

# Aria each core run how much cnt (M) per sec
# 0:read_cnt 1:write_cnt 2:commit_rw_step_cnt 3:commit_scan_step_cnt 4:commit_cnt 5:abort_cnt 6: exe_txn_cnt
time_costs = [1.2, 1.2, 0, 0, 2.0, 0, 0]

core_cnt = 16

dir_path = os.path.dirname(os.path.realpath(__file__))

print(os.path.basename(__file__), dir_path)

files = [f for f in listdir(dir_path) if f != os.path.basename(__file__)]

print(files)

times_files = []
abort_cnts_files = []
run_cnts_files = []


for file in files:
    file_path = dir_path + "\\" + file
    file1 = open(file_path, 'r')
    lines = file1.readlines()


    threads_cost = []
    batches_cost = []
    threads_abort_cnt = []
    threads_run_cnt = []
    
    times_file = []
    abort_cnts_file = []
    run_cnts_file = []

    batch_cnt = 1
    test_cnt = 1
    for i, line in enumerate(lines, start=0):

        

        if "batch" in line or "read_cnt" in line: # ok a batch
            if  threads_cost.__len__() != 0:
                # print(threads_cost)
                batch_cost = max(threads_cost)
                if len(threads_cost) > core_cnt:
                    batch_cost = batch_cost * len(threads_cost) / core_cnt
                batches_cost.append(batch_cost)
                threads_cost = []
                # print(str(batch_cnt) + " batch_cost: " + str(batch_cost))
                batch_cnt = batch_cnt + 1
            if "read_cnt" in line: # ok run
                if batches_cost.__len__() != 0:

                    times_file.append(sum(batches_cost))
                    abort_cnts_file.append(sum(threads_abort_cnt))
                    run_cnts_file.append(sum(threads_run_cnt))
                    # print(str(test_cnt) + " tps: " + str(sum(batches_cost)))
                    test_cnt = test_cnt + 1
                    batches_cost = []
                    batch_cnt = 1

                    # print(threads_run_cnt)
                    threads_abort_cnt = []
                    threads_run_cnt = []
        else:
            thread_cost = 0
            thread_op_list = [i.strip() for i in line.split()]
            for i in range(len(time_costs)):
                thread_cost = thread_cost + time_costs[i] * float(thread_op_list[i])
            threads_cost.append(thread_cost)
            threads_abort_cnt.append(int(thread_op_list[5]))
            threads_run_cnt.append(int(thread_op_list[6]))

    # tpss.append
    # print(str(test_cnt) + " tps: " + str(sum(batches_cost)))
    times_file.append(sum(batches_cost))
    abort_cnts_file.append(sum(threads_abort_cnt))
    run_cnts_file.append(sum(threads_run_cnt))

    times_files.append(times_file)
    abort_cnts_files.append(abort_cnts_file)
    run_cnts_files.append(run_cnts_file)

print("thd_cnt\ttps_avg\ttps_dev\tabort_rate_avg\tabort_rate_dev")
for thd_cnt in range(times_files[0].__len__()):
    tpss = []
    abort_rate = []
    for file in range(times_files.__len__()):
        tpss.append(core_run_op / times_files[file][thd_cnt])
        abort_rate.append(float(float(abort_cnts_files[file][thd_cnt]) / run_cnts_files[file][thd_cnt]))
    print(thd_cnt, mean(tpss), stdev(tpss), mean(abort_rate), stdev(abort_rate))

            
# print(i, mean(tps_list), stdev(tps_list), mean(conflict_list), stdev(conflict_list), mean(block_list), stdev(block_list))




            

    


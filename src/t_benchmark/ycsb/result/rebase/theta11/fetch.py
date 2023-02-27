

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

thetas = [0.9]
thd_cnt = 16

times_files = []
conflicts_files = []
blocks_files = []

rebase_rate = []

for file in files:
    file_path = dir_path + "\\" + file
    file1 = open(file_path, 'r')
    lines = file1.readlines()

    # print(lines[2].strip())

    for line in lines:
        if "read_cnt" in line:
            break
        else:
            rebase_rate.append(float(line.strip()))



print(mean(rebase_rate), stdev(rebase_rate))
    



            

    



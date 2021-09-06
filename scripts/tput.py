# -*- coding: utf-8 -*-
"""
Created on Tue May  3 09:53:43 2016

@author: khiem
"""
import sys

path_prefix = "/proj/cops/khiem/slowdown/experiments/latest/"
min_t = float("inf")
sys_tput_map = dict()
tput_interval_in_sec = 1.0


def process_line(line):
    global min_t
    tokens = line.split(str.encode("\t"))

    k = int((float(tokens[4]) - min_t) / tput_interval_in_sec)
    if k in sys_tput_map:
        tputs = sys_tput_map[k]
        tputs[0] += int(tokens[1])
        tputs[1] += int(tokens[2])
        tputs[2] += int(tokens[3])
    else:
        sys_tput_map[k] = [int(tokens[1]), int(tokens[2]), int(tokens[3])]


def process_single_client_tput(filename):
    with open(filename, "rb") as f:
        lines = f.readlines()

    # remove last two lines that print untrimmed/trimmed overall tputs
    lines = lines[0:-2]
    for line in lines:
        process_line(line)


def process_all_clients_tput(num_clients):
    for i in range(num_clients):
        process_single_client_tput(path_prefix + "client-" + str(i) + ".throughput.txt")


def write_dict_to_file(filename):
    global min_t
    f = open(filename, "w")
    for t in sorted(sys_tput_map.keys()):
        f.write(format(t * tput_interval_in_sec, '.1f') + "\t" + str(t) + "\t" + str(sys_tput_map[t][0]) + "\t" + str(
            sys_tput_map[t][1]) + "\t" + str(sys_tput_map[t][2]) + "\n")
    f.close()


def get_min_t(num_clients):
    global min_t
    for i in range(num_clients):
        with open(path_prefix + "client-" + str(i) + ".throughput.txt", "rb") as f:
            line = f.readline()
        tokens = line.split(str.encode("\t"))
        if min_t > float(tokens[4]):
            min_t = float(tokens[4])


if __name__ == "__main__":
    num_clients = int(sys.argv[1])
    tput_interval_in_sec = float(sys.argv[2])
    if len(sys.argv) > 3:
        path_prefix = str(sys.argv[3])
    get_min_t(num_clients)
    process_all_clients_tput(num_clients)
    write_dict_to_file(path_prefix + "sys_tput.txt")

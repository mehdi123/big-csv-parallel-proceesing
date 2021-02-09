#!/usr/bin/python3
import sys
from multiprocessing import Pool, Queue, Process
from multiprocessing import Lock
import time

averages = Queue()
plines = []

def put_average(average):
    averages.put(average)

def func(_id, field_index, start_index, end_index):
    total = 0
    for line in plines[start_index:end_index]:
        values = line.split(',')
        total += float(values[field_index])
    put_average(total/len(plines))

def p_run():
    global plines
    plines = open(sys.argv[1], 'rt', encoding='utf-8').readlines()
    SEPARATOR = ','
    header = plines[0]
    total_length = len(plines[1:])
    field_index = header.split(SEPARATOR).index('tip_amount')
    step = int(total_length / 5)
    procs = []
    for i in range(5):
        proc = Process(target=func, args=(i, field_index, i*step+1, (i+1)*step))
        proc.start()
        procs.append(proc)
    for proc in procs:
        proc.join()
    total = []
    for i in range(averages.qsize()):
        total.append(averages.get())
    print(sum(total)/len(total))

def s_run():
    lines = open(sys.argv[1], 'rt', encoding='utf-8').readlines()
    SEPARATOR = ','
    header = lines[0]
    total = 0
    field_index = header.split(SEPARATOR).index('tip_amount')
    for line in lines[1:]:
        total += float(line.split(SEPARATOR)[field_index])
    print(total/len(lines[1:]))

if __name__ == '__main__':
    t0 = time.time()
    s_run()
    print('serial', time.time()-t0)
    t0 = time.time()
    p_run()
    print('parallel', time.time()-t0)
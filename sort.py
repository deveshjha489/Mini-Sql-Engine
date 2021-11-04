import math
import heapq
import os
import sys
import time
from collections import OrderedDict
from operator import itemgetter


METADATA_FILE = 'metadata.txt'
metadata = OrderedDict()
col_map = dict()

class heapnode(object):
    def __init__(self, val, partition_no,colList,order):
        self.val = val
        self.partition_no = partition_no
        self.colList = colList
        self.order = order
    def __lt__(self, other):
        for i in self.colList:
            if self.order == 'ASC' and self.val[i] != other.val[i] :
                return self.val[i] < other.val[i]
            elif self.order == 'DESC' and self.val[i] != other.val[i] :
                return self.val[i] > other.val[i] 
            else:
                continue


def processMetadata():
    
    try:
        with open(METADATA_FILE, 'r') as meta:
            data = meta.readlines()
            ind = 0
            for row in data:
                line = row.strip('\n').split(',')
                col_name = line[0].strip()
                col_size = int(line[1].strip())
                metadata[col_name] = col_size
                col_map[col_name] = ind
                ind += 1
    except FileNotFoundError:
        print(METADATA_FILE + ' does not exist')
        sys.exit(0)

def split_info(availMemory,inputFile):
    
    tupleSize = 0
    tupleInInput = 0

    try:
        with open(inputFile,'r') as input:
            for tup in input:
                tupleInInput += 1
    except FileNotFoundError:
        print(inputFile + ' does not exist!')
        sys.exit(0)

    for col in metadata.keys():
        tupleSize += metadata[col]
    
    # print(tupleSize)
    tuple_in_file = min(tupleInInput,math.floor(availMemory/tupleSize))
    num_partition = math.ceil(tupleInInput/tuple_in_file)

    if tuple_in_file < num_partition:
        print('Invalid condition')
        sys.exit(0)

    #print("no of tuples in input: ",tupleInInput)
    #print("no of tuples in one file: ",tuple_in_file)
    print("no of sub files: ",num_partition)
    return [tuple_in_file,num_partition,tupleInInput]

def split_data(inputFile,splitInfo,order,col_ind):
    print('starting Phase-1')
    partition = 1
    block = []
    tuple_read = 0
    try:
        with open(inputFile,'r') as input:
            tuple_count = 0
            while(True):
                data = input.readline()
                
                if not data:
                    break
                
                tuple_read += 1

                curr_tuple = []
                curr_ind = 0
                #print('cols : ',metadata.keys())
                for col in metadata.keys():
                    bytes_to_read = metadata[col]
                    #print('curr byte : ', data[curr_ind:curr_ind + bytes_to_read])
                    curr_tuple.append(data[curr_ind:curr_ind + bytes_to_read]) ## aabb  cccc
                    curr_ind += bytes_to_read + 2 # 0 + 1 + 2 , 3 + 2

                block.append(curr_tuple)
                tuple_count += 1
                # print('block :',block)
                if tuple_read == splitInfo[2] or tuple_count == splitInfo[0]:
                    print("sorting sub-file #" + str(partition))
                    if(order == 'ASC'):
                        block.sort(key=itemgetter(*col_ind))
                    else:
                        block.sort(reverse=True,key=itemgetter(*col_ind))
                    print("writing sub-file #" + str(partition) + ' to disk')
                    with open(str(partition)+'.txt','w') as out: 
                        for ro in block:
                            s = ''
                            for co in ro:
                                s += str(co) + '  '
                            out.write(s)
                            out.write('\n')
                    block.clear()
                    partition += 1
                    tuple_count = 0
    except FileNotFoundError:
        print(inputFile + ' does not exist!')
        sys.exit(0)


def merge_partitions(outputFile,num_partition,tuple_in_file,col_ind,order):
    print('Starting Phase - 2')
    partitions_ptr = []

    for i in range(1,num_partition+1):
        ptr = open(str(i)+'.txt','r')
        partitions_ptr.append(ptr)

    ## If no of partitions becomes greater than no. of tuples that can fit in main memory -->ERROR

    partitions_no = 1
    minHeap = []
    for part in partitions_ptr:
        row = part.readline().strip('\n').split('  ')
        row = heapnode(row,partitions_no,col_ind,order)   
        heapq.heappush(minHeap,row)
        partitions_no += 1
    
    # for i in minHeap:
    #     print(i.val,end=' ')
    #     print(i.partition_no)

    # os.remove(outputFile) ## remove old output file with same name
    sorted_output_file = open(outputFile,'w')
    

    print('Writing to Disk')
    while len(minHeap):
        root = heapq.heappop(minHeap)
        row = root.val
        temp = ''
        for val in row:
            temp += val + '  '

        sorted_output_file.write(temp.rstrip()) ## format to str
        sorted_output_file.write('\n')
        next_elem = partitions_ptr[root.partition_no - 1].readline()
        if not next_elem:
            continue 
        curr_tuple = []
        curr_ind = 0
        #print('cols : ',metadata.keys())
        for col in metadata.keys():
            bytes_to_read = metadata[col]
            #print('curr byte : ', data[curr_ind:curr_ind + bytes_to_read])
            curr_tuple.append(next_elem[curr_ind:curr_ind + bytes_to_read]) ## aabb  cccc
            curr_ind += bytes_to_read + 2 # 0 + 1 + 2 , 3 + 2
        node = heapnode(curr_tuple,root.partition_no,col_ind,order)
        heapq.heappush(minHeap,node)

    for i in partitions_ptr:
        i.close()
    
    sorted_output_file.close()

def getArgs():

    if(len(sys.argv[1:]) < 5):
        print('Number of arguments should be atleast 5')
        sys.exit(0)

    try:
        inputFile = sys.argv[1]
        outputFile = sys.argv[2]
        availMemory = int(sys.argv[3])*1024*1024 ## Bytes
        order = (sys.argv[4]).upper()
        col_ind = sys.argv[5:]
    except:
        print('Invalid arguments')
        sys.exit(0)
    processMetadata()

    if order != 'ASC' and order != 'DESC':
        print(order + ' is invalid order, only asc or desc are valid')
        sys.exit(0)
    print("Start Execution")
    splitInfo = split_info(availMemory,inputFile) #availmem,inputfie  [tuple_in_file,num_partition]
    col_ind = [col_map[col] for col in col_ind]
    split_data(inputFile,splitInfo,order,col_ind)
    merge_partitions(outputFile,splitInfo[1],splitInfo[0],col_ind,order)


start = time.time()
getArgs()
end = time.time()
print('time elapsed : ', (end-start))
print('Finished execution')

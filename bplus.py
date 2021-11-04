import math
import sys
import bisect

order = 3 #Max number of child pointer, #keys = 2 , #ptr = 3
numKeys = order - 1
root = None

class internalNode:

    def __init__(self,childs=None,keys=None,leaf=False,keyCount=0):
        self.childs = list()
        self.keys = list()
        self.leaf = leaf
        self.parent = None
        self.keyCount = keyCount

class leafnode:

    def __init__(self,parent=None,keys=None,nxtPtr=None,leaf=True,keyCount=0):
        self.keys = list()
        self.nxtPtr = nxtPtr
        self.leaf = leaf
        self.keyCount = keyCount
        self.parent = parent



def insertKey(node,key):
    #print('inside insert key')
    global root
    if node == None:
        node = leafnode()
        root = node
        node.keys.append(key)
        node.keyCount += 1
    elif node.leaf:
        bisect.insort(node.keys, key)
        node.keyCount += 1
        if node.keyCount > numKeys:
            splitleaf(node,key)
    else:
        for i,val in enumerate(node.keys):
            if key < val:
                return insertKey(node.childs[i],key)
        return insertKey(node.childs[node.keyCount],key)

def splitleaf(node,key):
    # 1 2 3 8 10
    #print('inside split leaf for key:',key)
    numLeftKey = math.ceil(order/2)
    numRightKey = order - numLeftKey
    
    rightNode = leafnode()
    rightNode.keyCount = numRightKey
    rightNode.parent = node.parent
    rightNode.keys.extend(node.keys[numLeftKey:])
    rightNode.nxtPtr = node.nxtPtr

    node.keyCount = numLeftKey
    node.keys = [node.keys[i] for i in range(0,numLeftKey)]
    node.nxtPtr = rightNode

    #print(node.keys)
    #print(rightNode.keys)
    

    insertInternal(node,rightNode,node.keys[numLeftKey-1])

def insertInternal(leftnode,rightnode,key):
    global root
    
    if leftnode.parent == None and rightnode.parent == None:
        #print('first split')
        node = internalNode()
        node.keys.append(key)
        node.childs.append(leftnode)
        node.childs.append(rightnode)
        node.keyCount += 1
        leftnode.parent = node
        rightnode.parent = node
        root = node
        return
    else:
        currNode = leftnode.parent
        ind = currNode.childs.index(leftnode)
        currNode.childs.insert(ind+1,rightnode)
        bisect.insort(currNode.keys,key)
        currNode.keyCount += 1

        if currNode.keyCount > numKeys:
            #split internal node
            leftnode, rightnode, key = splitInternal(currNode)
            insertInternal(leftnode,rightnode,key)
        else:
            return

def splitInternal(node):
    #print('inside split internal')

    numLeftKey = math.ceil((order-1)/2)
    numRightKey = order - numLeftKey - 1
    mid_ind = math.floor(order/2)
    key = node.keys[mid_ind]
    rightNode = internalNode()
    rightNode.keyCount = numRightKey
    rightNode.parent = node.parent
    rightNode.keys.extend(node.keys[mid_ind+1:])
    rightNode.childs.extend(node.childs[mid_ind+1:])

    for ptr in node.childs[mid_ind+1:]:
        ptr.parent = rightNode

    node.keyCount = numLeftKey
    node.keys = [node.keys[i] for i in range(0,mid_ind)]
    node.childs = [node.childs[i] for i in range(0,mid_ind+1)]

    #rint(node.keys)
    #print(rightNode.keys)
    return node,rightNode,key

def find_x(node,key):
    if not node:
        return False
    
    if node.leaf:
        for val in node.keys:
            if val == key:
                return True
        return False

    for i,val in enumerate(node.keys):
        if val == key:
            return True
        elif key <= val:
            return find_x(node.childs[i],key)

    return find_x(node.childs[node.keyCount],key)    

def count_x(node,key):

    if not node:
        return 0

    if node.leaf:
        count = 0
        flag = True
        while node and flag:
            for val in node.keys:
                if val == key:
                    count+=1
                elif val > key:
                    flag = False
                    break
            node = node.nxtPtr
        return count

    for i,val in enumerate(node.keys):
        if key <= val:
            return count_x(node.childs[i],key)

    return count_x(node.childs[node.keyCount],key)

def range_query(node,l,r):
    
    if not node:
        return 0

    if node.leaf:
        count = 0
        flag = True
        while node and flag:
            for val in node.keys:
                if val>=l and val<=r:
                    count += 1
                elif val > r:
                    flag = False
                    break
            node = node.nxtPtr
        return count

    for i,val in enumerate(node.keys):
        if l <= val:
            return range_query(node.childs[i],l,r)
    return range_query(node.childs[node.keyCount],l,r)

def printNodes(node):

    for val in node.keys:
        print(val,end=',')
    if node.parent:
        print('parent=',node.parent.keys)
    else:
        print('parent=NULL')
    print('\n***\n')
    if not node.leaf:   
        for val in node.childs:
            if val != None:
                printNodes(val)

def process(query,out):
    global root
    query = query.split(' ')
    if query[0] == 'INSERT':
        insertKey(root,int(query[1]))
    elif query[0] == 'FIND':
        is_found = find_x(root,int(query[1]))
        if is_found:
            out.write('YES\n')
        else:
            out.write('NO\n')
    elif query[0] == 'COUNT':
        out.write(str(count_x(root,int(query[1]))) + '\n')
    else:
        out.write(str(range_query(root,int(query[1]),int(query[2]))) + '\n')


filename = sys.argv[1]

out = open('output.txt','w')

with open(filename,'r') as input:
    for line in input:
        query = line.rstrip('\n')
        process(query,out)

out.close()

# insertKey(root,1)
# insertKey(root,8)
# insertKey(root,10)
# insertKey(root,5)
# insertKey(root,2)

# print(find_x(root,1))
# print(find_x(root,8))
# print(find_x(root,10))
# print(find_x(root,5))
# print(find_x(root,2))
# print(find_x(root,15))
#printNodes(root)
import sqlparse
import sys
import re
from csv import reader
from collections import OrderedDict
from functools import cmp_to_key

## TODO's
# Handle cell value with "" or '' in csv in storeTable  func[DONE]
# Fix order by index error [DONE]
## error handling
#: table exist or not {DONE}
#: from clause missing {DONE}--
#: check if select col exist in table {DONE}
#: check if select keyword in query {DONE}--
#: if semicolon not present -->display error{DONE}---
###: case sensitive col,table name---
#: check for leading and trailing splace in col names in query {DONE}--
## remove directory for csv--

## where check col in table

METADATA_FILE = 'metadata.txt'
DIR = 'files'

cols = []
dbTable = []
tableOrder = {}

def isExistCol(colum, from_table):
    col_avail = set()
    for j in from_table:
        for cl in tableDict[j.lower().strip()].keys():
            col_avail.add(cl)
    #print(col_avail)
    if colum.upper() not in col_avail and colum != '*':
        print('Invalid Query : ' + colum + ' does not exist in table')
        sys.exit(0)
    else:
        return True

def getCols(token):
    ls = token.split(',') 
    global cols
    header = []
    for c in ls:
        header.append(c.strip())
    return header

def parseMetadata(dirname,filename,tableDict):
    meta_data = []
    try:
        with open(filename , 'r') as meta:
            meta_data = meta.read().split('\n')
    except FileNotFoundError:
        print('file with name' + filename + 'does not exist')
        sys.exit(0)
    if meta_data:
        i = 0
        size = len(meta_data) - 1 ## CHECK THIS
        while i < size:
            if(meta_data[i] == '<begin_table>'):
                i += 1
                table_name = meta_data[i]
                tableDict[table_name] = OrderedDict()
                i += 1
                while(meta_data[i] != '<end_table>'):
                    tableDict[table_name][meta_data[i].upper()] = []
                    i += 1
                i += 1
def comparator(a,b):
    if a[0] > b[0]:
        return 1
    if a[0] == b[0]:
        if a[1] < b[1]:
            return 1
    return -1

def orderBy(data,token,header):
    token = token.strip().split(' ')
    col = token[0]
    pos = -1
    for i in range(0,len(header)):
        if header[i] == col:
            pos = i
            break
    
    if pos == -1:
        data.insert(0,header)
        return data

    if len(token) > 1:
        order = token[1]
    else:
        order = "ASC"

    val_ind = [] # col is coloumn name str
    for i in range(0,len(data)):
        val = data[i][pos] # get index of col in cross 
        val_ind.append([val,i])


    if order == 'ASC':
        val_ind = sorted(val_ind , reverse=False)
    else:
        val_ind = sorted(val_ind , key=cmp_to_key(comparator),reverse=True)

    ordered_list = []
    ordered_list.append(header)
    for item in val_ind:
        ordered_list.append(data[item[1]])

    return ordered_list

def groupBy(data,col):
    col = col.strip()
    groupBy_dict = OrderedDict()
    for i in range(0,len(data)):
        key = data[i][tableOrder[col]] # current row , group by col
        if key in groupBy_dict.keys():
            groupBy_dict[key].append(data[i])
        else:
            groupBy_dict[key] = []
            groupBy_dict[key].append(data[i])

    #print(groupBy_dict)
    return groupBy_dict

def transpose(tableList):
    tables = []
    
    for table in tableList:
        temp = []
        table = table.strip().lower()
        for key in tableDict[table].keys():
            temp.append(tableDict[table][key])
        tables.append(temp)
    
    i = 0
    while i < len(tables):
        temp = [list(i) for i in zip(*tables[i])]
        tables[i] = temp
        i += 1
    return tables

def cross(tables):
    lt = []
    for row in tables[0]: 
        delim = ' '
        temp = list(map(str,row))
        lt.append(delim.join(temp))
    
    i = 1
    final = []
    while i < len(tables):
        new = []
        for p in lt:
            init = p # 1 2
            for j in range(0,len(tables[i])):
                temp = ''
                for k in range(0,len(tables[i][0])):
                    temp += ' ' + str(tables[i][j][k]) # 2 3 4
                temp = init + temp # 1 2 2 3 4
                new.append(temp)
            lt = new
        i += 1
    #print(lt)
    for item in lt:
        item = list(map(int,item.split()))
        final.append(item)
    return final

def where(crossJoin,token,dbTable):
    #print(crossJoin)
    op = token.strip().strip(';').split(' ')
    
    temp = []
    
    for it in op:
        if it != '':
            temp.append(it.strip())

    op = temp

    filteredRow = []
    if ('AND' in token) or ('OR' in token):
        # val2 = ''
        # val4 = ''
        done1 = False
        done2 = False   
        if(re.match("[-+]?\d+$", op[3])):
            val2 = int(op[3])
            done1 = True
        else:
            val2 ='$'
        if(re.match("[-+]?\d+$", op[7])):
            val4 = int(op[7])
            done2 = True
        else:
            val4 = '$'

        for row in crossJoin:
            isExistCol(op[1],dbTable)
            val1 = row[tableOrder[op[1]]]
            isExistCol(op[5],dbTable)
            val3 = row[tableOrder[op[5]]]
            
            if not done1 :
                isExistCol(op[3],dbTable)
                val2 = row[tableOrder[op[3]]]
            if not done2 :
                isExistCol(op[7],dbTable)
                val4 = row[tableOrder[op[7]]]
            
            if (
                (op[4] == 'AND' and op[2] == '>' and op[6] == '>' and val1>val2 and val3>val4) or 
                (op[4] == 'OR' and op[2] == '>' and op[6] == '>' and (val1>val2 or val3>val4))
               ):
                #print('1')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '>' and op[6] == '<' and val1>val2 and val3<val4) or 
                (op[4] == 'OR' and op[2] == '>' and op[6] == '<' and (val1>val2 or val3<val4))
               ):
               #print('2')
               filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '>' and op[6] == '>=' and val1>val2 and val3>=val4) or 
                (op[4] == 'OR' and op[2] == '>' and op[6] == '>=' and (val1>val2 or val3>=val4))
               ):
                #print('3')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '>' and op[6] == '<=' and val1>val2 and val3<=val4) or 
                (op[4] == 'OR' and op[2] == '>' and op[6] == '<=' and (val1>val2 or val3<=val4))
               ):
                #print('4')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '>' and op[6] == '=' and val1>val2 and val3==val4) or 
                (op[4] == 'OR' and op[2] == '>' and op[6] == '=' and (val1>val2 or val3==val4))
               ):
                #print('5')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '<' and op[6] == '<' and val1<val2 and val3<val4) or 
                (op[4] == 'OR' and op[2] == '<' and op[6] == '<' and (val1<val2 or val3<val4))
               ):
                #print('6')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '<' and op[6] == '>' and val1<val2 and val3>val4) or 
                (op[4] == 'OR' and op[2] == '<' and op[6] == '>' and (val1<val2 or val3>val4))
               ):
                #print('7')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '<' and op[6] == '>=' and val1<val2 and val3>=val4) or 
                (op[4] == 'OR' and op[2] == '<' and op[6] == '>=' and (val1<val2 or val3>=val4))
               ):
                #print('8')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '<' and op[6] == '<=' and val1<val2 and val3<=val4) or 
                (op[4] == 'OR' and op[2] == '<' and op[6] == '<=' and (val1<val2 or val3<=val4))
               ):
                #print('9')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '<' and op[6] == '=' and val1<val2 and val3==val4) or 
                (op[4] == 'OR' and op[2] == '<' and op[6] == '=' and (val1<val2 or val3==val4))
               ):
                #print('10')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '>=' and op[6] == '>' and val1>=val2 and val3>val4) or 
                (op[4] == 'OR' and op[2] == '>=' and op[6] == '>' and (val1>=val2 or val3>val4))
               ):
                #print('11')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '>=' and op[6] == '<' and val1>=val2 and val3<val4) or 
                (op[4] == 'OR' and op[2] == '>=' and op[6] == '<' and (val1>=val2 or val3<val4))
               ):
                #print('12')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '>=' and op[6] == '>=' and val1>=val2 and val3>=val4) or 
                (op[4] == 'OR' and op[2] == '>=' and op[6] == '>=' and (val1>=val2 or val3>=val4))
               ):
                #print('13')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '>=' and op[6] == '<=' and val1>=val2 and val3<=val4) or 
                (op[4] == 'OR' and op[2] == '>=' and op[6] == '<=' and (val1>=val2 or val3<=val4))
               ):
                #print('14')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '>=' and op[6] == '=' and val1>=val2 and val3==val4) or 
                (op[4] == 'OR' and op[2] == '>=' and op[6] == '=' and (val1>=val2 or val3==val4))
               ):
                #print('15')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '<=' and op[6] == '>' and val1<=val2 and val3>val4) or 
                (op[4] == 'OR' and op[2] == '<=' and op[6] == '>' and (val1<=val2 or val3>val4))
               ):
                #print('16')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '<=' and op[6] == '<' and val1<=val2 and val3<val4) or 
                (op[4] == 'OR' and op[2] == '<=' and op[6] == '<' and (val1<=val2 or val3<val4))
               ):
                #print('17')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '<=' and op[6] == '>=' and val1<=val2 and val3>=val4) or 
                (op[4] == 'OR' and op[2] == '<=' and op[6] == '>=' and (val1<=val2 or val3>=val4))
               ):
                #print('18')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '<=' and op[6] == '<=' and val1<=val2 and val3<=val4) or 
                (op[4] == 'OR' and op[2] == '<=' and op[6] == '<=' and (val1<=val2 or val3<=val4))
               ):
                #print('19')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '<=' and op[6] == '=' and val1<=val2 and val3==val4) or 
                (op[4] == 'OR' and op[2] == '<=' and op[6] == '=' and (val1<=val2 or val3==val4))
               ):
                #print('20')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '=' and op[6] == '>' and val1==val2 and val3>val4) or # WHERE a = d or D = f;'
                (op[4] == 'OR' and op[2] == '=' and op[6] == '>' and (val1==val2 or val3>val4))
               ):
                #print('21')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '=' and op[6] == '<' and val1==val2 and val3<val4) or 
                (op[4] == 'OR' and op[2] == '=' and op[6] == '<' and (val1==val2 or val3<val4))
               ):
                #print('22')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '=' and op[6] == '>=' and val1==val2 and val3>=val4) or 
                (op[4] == 'OR' and op[2] == '=' and op[6] == '>=' and (val1==val2 or val3>=val4))
               ):
                #print('23')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '=' and op[6] == '<=' and val1==val2 and val3<=val4) or 
                (op[4] == 'OR' and op[2] == '=' and op[6] == '<=' and (val1==val2 or val3<=val4))
               ):
                #print('24')
                filteredRow.append(row)
            elif (
                (op[4] == 'AND' and op[2] == '=' and op[6] == '=' and val1==val2 and val3==val4) or 
                (op[4] == 'OR' and op[2] == '=' and op[6] == '=' and (val1==val2 or val3==val4))
               ):
               #print(str(val1) + '=' + str(val2) + ',' + str(val3) + '=' + str(val4))
               filteredRow.append(row)
    else:
        # A >= B , A >=2
        col = op[1]
        # print('hello')
        isExistCol(col,dbTable)
        relop = op[2]
        s = op[3]
        col = tableOrder[col.strip()]
        relop = relop.strip()
       #  print(col)
        #print(s)
        for row in crossJoin:
            val1 = row[col]
            if(re.match("[-+]?\d+$", s)):
                val2 = int(s)
            else:
                isExistCol(s,dbTable) ###
                col2 = tableOrder[s.strip()] 
                val2 = row[col2]
            if relop == '>' and val1 > val2:
                filteredRow.append(row)
            elif relop == '<' and val1 < val2:
                filteredRow.append(row)
            elif relop == '>=' and val1 >= val2:
                filteredRow.append(row)
            elif relop == '<=' and val1 <= val2:
                filteredRow.append(row)
            elif relop == '=' and val1 == val2:
                filteredRow.append(row)
    return filteredRow


def printResult(result,colTableMapping):
    for row in range(0,len(result)):
        for rr in range(0,len(result[row])):
            if(row == 0):
                curr = result[row][rr]
                if '(' in curr:
                    print(curr.lower(),end='')
                else:
                    print(colTableMapping[curr].lower() + '.' + curr.lower(),end='')
            else:
                print(result[row][rr],end = '')
            if(rr != len(result[row])-1):
                print(',',end='')
        print()

def processQuery(tokenList):
    isDistinct = False
    isGroupBy = False
    isOrderBy = False
    isAggr = False
    isWildcard = False
    next_col = False
    next_from = False
    next_table = False
    crossJoin = []
    header = []
    order = ''
    global cols
    global dbTable
    global tableOrder
    for token in range(0,len(tokenList)):
        if(tokenList[token] == 'SELECT'):
            next_col = True
        elif(tokenList[token] == 'DISTINCT'):
            isDistinct = True
        elif(next_col):
            if tokenList[token] == '*':
                isWildcard = True
            header = getCols(tokenList[token])
            next_from = True
            next_col = False
        elif(next_from):
            if tokenList[token] != 'FROM':
                print('Invalid Query : From clause missing')
                sys.exit(0)
            next_table = True
            next_from = False
        elif(next_table):
            dbTable = tokenList[token].split(',')
            dbTable = [tb.strip().lower() for tb in dbTable]
            for tb in dbTable:
                if tb.lower().strip() not in tableDict.keys(): ## CASE
                    print('Invalid Query :' + tb.lower() + ' table does not exist')
                    sys.exit(0)
            ind = 0
            crossJoin = cross(transpose(dbTable))
            for table in dbTable:
                table = table.strip().lower()
                for key in tableDict[table].keys():
                   tableOrder[key] = ind
                   ind += 1
            for head in header: ## CASE
                if '(' in head:
                    colum = head.split('(')[1].split(')'[0])
                    colum = colum[0].upper()
                else:
                    colum = head.strip().upper()
                # colum_list_from = set()
                # for i in dbTable:
                #     temp = list(tableDict[i.lower()].keys())
                #     for j in temp:
                #         colum_list_from.add(j)
                # print(colum)
                if not isExistCol(colum,dbTable) and '*' not in head: # if colum not in colum_list_from and '*' not in head:
                    print('Invalid Query : ' + head + ' column does not exist')
                    sys.exit(0)
            next_table = False
        
        elif 'WHERE' in tokenList[token]:
            crossJoin = where(crossJoin,tokenList[token],dbTable)

        elif 'GROUP BY' in tokenList[token]:
            isGroupBy = True
            groupBy_col = tokenList[token+1]
            group_order = groupBy(crossJoin,groupBy_col)
        elif 'ORDER BY' in tokenList[token]:
            isOrderBy = True
            order = tokenList[token+1]
    # SELECT a,max(a),max(b) from table.........
    toPrint = []
    colums = False # all cols
    toPrint.append(header) # group by A [[A,max(B),count(C)],[1,2,4...]]
    if isGroupBy:
        for c in group_order.keys(): # C = group by col
            temp = []
            for cl in header: 
                if '(' in cl: #it's a aggregate func
                    agg = cl.split('(') #(max,col)
                    func = agg[0].strip()
                    aggcol = agg[1].strip().split(')')[0] # col
                    # print(func + ' ' + aggcol)
                    if func == 'SUM':
                        sumCol = 0
                        for row in group_order[c]:
                            sumCol += row[tableOrder[aggcol]]
                        temp.append(sumCol)
                    if func == 'AVG':
                        average = 0
                        total = len(group_order[c])
                        for row in group_order[c]:
                            average += row[tableOrder[aggcol]]
                        temp.append(average/total)
                    if func == 'MAX':
                        mx = -2147483648
                        for row in group_order[c]:
                            mx = max(mx,row[tableOrder[aggcol]])
                        temp.append(mx)
                    if func == 'MIN' :
                        mi = 2147483647
                        for row in group_order[c]:
                            mi = min(mi,row[tableOrder[aggcol]])
                        temp.append(mi)
                    if func == 'COUNT':
                        temp.append(len(group_order[c]))
                else:
                    if cl != groupBy_col:
                        print('Invalid Query : Group by col ' + cl + ' does not exist in SELECT')
                        sys.exit(0)
                    temp.append(c)
            toPrint.append(temp)
    else:
        temp = []
        for cl in header: 
            if '(' in cl: #it's a aggregate func
                isAggr = True
                agg = cl.split('(') # max , col)
                func = agg[0].strip()
                aggcol = agg[1].strip().split(')')[0] # col
                if func == 'SUM':
                    sumCol = 0
                    for row in crossJoin:
                        sumCol += row[tableOrder[aggcol]]
                    temp.append(sumCol)
                if func == 'AVG':
                    average = 0
                    total = len(crossJoin)
                    for row in crossJoin:
                        average += row[tableOrder[aggcol]]
                    temp.append(average/total)
                if func == 'MAX':
                    mx = -2147483648
                    for row in crossJoin:
                        mx = max(mx,row[tableOrder[aggcol]])
                    temp.append(mx)
                if func == 'MIN' :
                    mi = 2147483647
                    for row in crossJoin:
                        mi = min(mi,row[tableOrder[aggcol]])
                    temp.append(mi)
                if func == 'COUNT':
                    temp.append(len(crossJoin))
            else:
                colums = True
        toPrint.append(temp)

    if(isAggr and colums):
        print('Invalid query : In aggregated query without GROUP BY, SELECT list contains nonaggregated column')
        sys.exit(0)

    if not isAggr and not isGroupBy:
        toPrint.clear()
        if isWildcard :
            header = list(tableOrder.keys())
        toPrint.append(header)
        for row in crossJoin:
            temp = []
            for head in header:
                temp.append(row[tableOrder[head]])
            toPrint.append(temp)

    if isDistinct:
        distinct = set()
        exclude = []
        last = len(toPrint)
        for i in range(0,last):
            if tuple(toPrint[i]) in distinct:
                exclude.append(i)
            else:
                distinct.add(tuple(toPrint[i]))
        temp = []
        for i in range(0,last):
            if i not in exclude:
                temp.append(toPrint[i])
        toPrint = temp

    if isOrderBy:
        toPrint = orderBy(toPrint[1:],order,header)

    return toPrint

def storeTable(dirname,numTable,tableDict):
    i = 0
    prefix = list(tableDict.keys())
    while i < numTable:
        table_name = prefix[i]
        filename = table_name + '.csv'
        with open(filename , 'r' ) as table:
            data = reader(table) #  [1 2 3]
            keys = list(tableDict[table_name].keys()) # [A B C]
            for row in data:
                j = 0
                while j < len(keys):
                    row[j] = row[j].strip("'")
                    row[j] = row[j].strip("\"")
                    tableDict[table_name][keys[j]].append(int(row[j]))
                    j += 1
        i += 1

def tokenize(sql):
    tokens_list = []
    for st in sql:
        if(str(st) != ' '):
            tokens_list.append(str(st).upper())
    return tokens_list

sql = ' '.join(sys.argv[1].split(None))
if sql[len(sql)-1] != ';':
    print('Invalid query : missing semicolon ;')
    sys.exit(0)
parsed = sqlparse.parse(sql)
tokens_list = tokenize(parsed[0].tokens)

if(tokens_list[0] != 'SELECT'):
    print('Invalid query : select statement missing')
    sys.exit(0)

tableDict = OrderedDict()
parseMetadata(DIR,METADATA_FILE,tableDict)
colTableMapping = dict()
for key in tableDict.keys():
    for subkey in tableDict[key].keys():
        colTableMapping[subkey] = key


numTable = len(tableDict)
storeTable(DIR,numTable,tableDict)
result = processQuery(tokens_list)


printResult(result,colTableMapping)
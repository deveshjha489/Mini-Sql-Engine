#include<bits/stdc++.h>
#include<fstream>
#include<chrono>
#define tupleInBlock 100
#define prime 31
using namespace std;
using namespace std::chrono; 

int numBlockInMem;
string rPath,sPath,joinType,outputFile;

class heapNode{
    public:
        string y;
        string tuple;
        int relation;
        int sublistNum;

        heapNode(){
            tuple = y = "";
            relation = sublistNum = -1;
        }

        heapNode(string y, string tuple, int relation, int sublistNum){
            this->tuple = tuple;
            this->y = y;
            this->relation = relation;
            this->sublistNum = sublistNum;
        }
};

bool comp_R(vector<string> &a, vector<string> &b){
    return a[1] <= b[1];
}


struct Compare {
    bool operator()(heapNode *p1, heapNode *p2)
    {   
        return p1->y >= p2->y;
    }
};

string getFilename(string path){
    string filename = "";
    for(int i=path.length()-1;i>=0;i--){
        if(path[i] != '\\' && path[i]!='/'){
           // clog<<path[i]<<endl;
            filename += path[i];
        }else{
            break;
        }
    }

    reverse(filename.begin(),filename.end());
    return filename;
}

void validatePath(string path){

    ifstream check;
    check.open(path);
    if(!check.good()){
        cout<<"Invalid File path. Exiting Program\n";
        exit(-1);
    }

}

vector<string> splitString(string str,char separator){
    string temp = "";
    vector<string> split;
    for(auto c : str){
        if(c != separator){
            temp += c;
        }else{
            split.push_back(temp);
            temp = "";
        }
    }
    split.push_back(temp);
    return split;
}

void writeSublist(int sublistNumber,string relationName,vector<vector<string>> &tuples){
        //clog<<"Writing sublist of relation:"<<relationName<<endl;
        string sublist_name = relationName + to_string(sublistNumber) + ".txt";
        ofstream out;
        out.open(sublist_name);
        string temp = "";
        for(int row=0;row<tuples.size();row++){
            if(row != tuples.size()-1)
                temp = tuples[row][0] + " " + tuples[row][1] + "\n";
            else temp = tuples[row][0] + " " + tuples[row][1];
            out<<temp;
        }
        tuples.clear();
        out.close();
}

pair<int,int>  createSortedSublist(string inputPath,string relationName){
    
    //clog<<"Creating sorted sublist for "<<relationName<<endl;

    ifstream in;
    in.open(inputPath);
    //clog<<"opened:"<<inputPath<<endl;
    string t;
    int numSublistsInRelation = 0, currTupleCount = 0, numberOfBlock = 0, numTuple = 0;

    vector<vector<string>> tuples;

    while(getline(in,t)){
       
        numTuple++;
        vector<string> tuple = splitString(t,' ');
        currTupleCount++;

        tuples.push_back(tuple);
        if(currTupleCount == numBlockInMem*tupleInBlock){
            //sort and write split to sublist
            //clog<<"tuples read:"<<currTupleCount<<endl;
            if(relationName == "R"){
                sort(tuples.begin(),tuples.end(),comp_R);
            }
            else{
                sort(tuples.begin(),tuples.end());
            }
            currTupleCount = 0;
            writeSublist(numSublistsInRelation,relationName,tuples);
            numSublistsInRelation++;
        }
    }

    if(tuples.size()){
        if(relationName == "R"){
            sort(tuples.begin(),tuples.end(),comp_R);
        }else{
            sort(tuples.begin(),tuples.end());
        }
        writeSublist(numSublistsInRelation,relationName,tuples);
        numSublistsInRelation++;
    }

    //clog<<"num of sublist in relation "<<relationName<<" is:"<<numSublistsInRelation<<endl;
    numberOfBlock = (numTuple + tupleInBlock - 1)/tupleInBlock; // ceil of numTuple/tupleInBlock
    in.close();
    //clog<<"Blocks in MM:"<<numBlockInMem<<endl;
    //clog<<"Blocks:"<<numberOfBlock<<endl;
    pair<int,int> info = {numSublistsInRelation,numberOfBlock};
    return info;
}

void openSublist(vector<ifstream> &fd, string relationName, int numSublist){
    //clog<<"opening sublist of relation "<<relationName<<endl;
    //clog<<"fd size:"<<numSublist<<endl;
    for(int i=0;i<numSublist;i++){
        string sublist_name = relationName + to_string(i) + ".txt";
        fd.push_back(ifstream(sublist_name));
    }
}


void initHeap(priority_queue<heapNode*,vector<heapNode*>,Compare> &heap,int sublist, vector<ifstream> &ptr,int rel){
    //clog<<"initHeap for relation "<<rel<<endl;
    string tuple;
    //clog<<"ptr.size():"<<ptr.size()<<endl;
    for(int sublist=0; sublist<ptr.size(); sublist++){
        for(int tup=0; tup<tupleInBlock && !ptr[sublist].eof(); tup++){
            getline(ptr[sublist],tuple);
            //clog<<"tuple:"<<tuple<<endl;
            heapNode *node = NULL;
            vector<string> col_val = splitString(tuple,' ');
            if(rel == 0)
                node = new heapNode(col_val[1],tuple,rel,sublist); //X Y
            else 
                node = new heapNode(col_val[0],tuple,rel,sublist); // Y Z
            heap.push(node);
        }
    }
}

void printHeap(priority_queue<heapNode*,vector<heapNode*>,Compare> heap){
    
    while(!heap.empty()){
        heapNode *node = heap.top();
        if(node->relation == 0)
            clog<<node->y<<" "<<node->tuple<<" "<<node->relation<<" "<<node->sublistNum<<endl;
        else
            clog<<node->y<<" "<<node->tuple<<" "<<node->relation<<" "<<node->sublistNum<<endl;
        heap.pop();
    }

}

void sublistClose(vector<ifstream> &ptr){

    for(auto i = ptr.begin(); i!= ptr.end(); i++){
        (*i).close();
    }

}

void mergeJoin(int R_sublist, int S_sublist){
    // X,Y,R/S,S#
    //clog<<"merge Join started"<<endl;

    priority_queue<heapNode*,vector<heapNode*>,Compare> heap; // R = 0 , S = 1

    vector<ifstream> r_ptr, s_ptr;

    openSublist(r_ptr,"R",R_sublist);
    openSublist(s_ptr,"S",S_sublist);

    initHeap(heap,R_sublist,r_ptr,0);
    // clog<<"*********************RRRRR***************************\n";
    // printHeap(heap);
    // clog<<"*********************RRRRR***************************\n";
    initHeap(heap,S_sublist,s_ptr,1);
    // clog<<"*********************SSSSS***************************\n";
    // printHeap(heap);
    // clog<<"*********************SSSSS***************************\n";

    ofstream out;
    out.open(outputFile);

    string last = "";
    vector<string> r, s;

    while(!heap.empty()){
        string row;
        heapNode *currNode = heap.top();
        heap.pop();
        
        if(currNode->relation == 0){
            //clog<<"curr node:"<<currNode->x<<" "<<currNode->y<<endl;
            row = currNode->tuple;
        }
        else{
            //clog<<"curr node:"<<currNode->y<<" "<<currNode->x<<endl;
            row = currNode->tuple;
        }
        //clog<<"[last:"<<last<<", currnode:"<<currNode->tuple<<"]"<<endl;
        if(currNode->y != last && last != ""){
            last = currNode->y;

            if(s.size() !=0 && r.size()!=0){
                //clog<<"writing output"<<endl;
                for(int i=0;i<r.size();i++){
                    for(int j=0;j<s.size();j++){
                        string X = splitString(r[i],' ')[0];
                        string Y = splitString(r[i],' ')[1];
                        string Z = splitString(s[j],' ')[1];
                        X = X + " " + Y + " " + Z;
                        out<<X<<endl;
                    }
                }
            }
            r.clear();
            s.clear();
        }


        if(currNode->relation == 0){
            r.push_back(row);    
            string nxt;
            if(getline(r_ptr[currNode->sublistNum],nxt)){
                vector<string> temp = splitString(nxt,' ');
                //clog<<"[push:"<<temp[0]<<" "<<temp[1]<<"]"<<endl;
                heap.push(new heapNode(temp[1],nxt,0,currNode->sublistNum));     
                //printHeap(heap); 
            }   
        }else{
            s.push_back(row);
            string nxt;
            if(getline(s_ptr[currNode->sublistNum],nxt)){
                vector<string> temp = splitString(nxt,' ');
                //clog<<"[push:"<<temp[0]<<" "<<temp[1]<<"]"<<endl;
                heap.push(new heapNode(temp[0],nxt,1,currNode->sublistNum));      
                //printHeap(heap); 
            }  
        }
        last = currNode->y;
    }

    if(r.size() && s.size()){
        //clog<<"Merge remaining\n";
        for(int i=0;i<r.size();i++){
            for(int j=0;j<s.size();j++){
                string X = splitString(r[i],' ')[0];
                string Y = splitString(r[i],' ')[1];
                string Z = splitString(s[j],' ')[1];
                X = X + " " + Y + " " + Z;
                out<<X<<endl;
            }
        }
    }

    sublistClose(r_ptr);
    sublistClose(s_ptr);
    out.close();

    //cout<<"merge join ended"<<endl;
}

void sortJoin(){
    //  create sorted sublist of R and S.
    pair<int,int>  R_sublist = createSortedSublist(rPath,"R"); // Number of sublist in relation R , Num of blocks in R
    pair<int,int>  S_sublist = createSortedSublist(sPath,"S"); // Number of sublist in relation S,  Num of blocks in S
    int total = R_sublist.second + S_sublist.second;
    int mx = numBlockInMem*numBlockInMem;
    if(total > mx){
        clog<<"Insufficient memory buffers\n";
        exit(-1);
    }
    // clog<<"Blocks in MM:"<<numBlockInMem<<endl;
    // clog<<"Blocks in R:"<<R_sublist.second<<endl;
    // clog<<"Blocks in S:"<<S_sublist.second<<endl;
    mergeJoin(R_sublist.first,S_sublist.first);
}


int hashY(string val){
    
    long long hash = 0 , p = 1;
    for(auto c : val){
        hash += c*p;
        hash = hash%numBlockInMem;
        p *= (long long)prime;
    }

    //clog<<"string : "<<val<<",hash :"<<hash<<endl;

    return hash;
}

void write_bucket(int bucket_num, map<int,pair<int,ofstream>> &ptr, string filename, string t,int &sublist){
    

    if(ptr.find(bucket_num) != ptr.end()){
        ptr[bucket_num].second<<t<<endl;
    }else{
        ptr[bucket_num].second = ofstream(filename); 
        ptr[bucket_num].second<<t<<endl;
        sublist++;
    }
    ptr[bucket_num].first++;
    
    if(ptr[bucket_num].first > numBlockInMem*100){
        clog<<"Insufficient Buffer Size\n";
        exit(-1);
    }
}

int createBuckets(string inputPath, string relationName,vector<int> &tupleCount){
    
    //clog<<"Creating Buckets  for "<<relationName<<endl;

    ifstream in;
    in.open(inputPath);
    //clog<<"opened:"<<inputPath<<endl;
    string t, filename;
    map<int,pair<int,ofstream>> ptr; //bucket# ,(tuples,filrptr)
    int numSublistsInRelation = 0, numberOfBlock = 0, numTuple = 0;

    vector<vector<string>> tuples;
    int bucket_num = 0;
    while(getline(in,t)){

        numTuple++;
        vector<string> tuple = splitString(t,' ');

        if(relationName == "R"){
            bucket_num = hashY(tuple[1]);
            filename = relationName + to_string(bucket_num) + ".txt";
            write_bucket(bucket_num,ptr,filename,t,numSublistsInRelation);
        }else{
            bucket_num = hashY(tuple[0]);
            filename = relationName + to_string(bucket_num) + ".txt";
            write_bucket(bucket_num,ptr,filename,t,numSublistsInRelation);
        }
    }

    for(auto it= ptr.begin(); it!=ptr.end(); it++){
        tupleCount[it->first] = (it->second).first;
    }


    //clog<<"num of buckets in relation "<<relationName<<" is:"<<numSublistsInRelation<<endl;
    numberOfBlock = (numTuple + tupleInBlock - 1)/tupleInBlock; // ceil of numTuple/tupleInBlock
    in.close();
    //clog<<"Blocks in MM:"<<numBlockInMem<<endl;
    //clog<<"Blocks:"<<numberOfBlock<<endl;

    return numSublistsInRelation;
}

void hashOutput(int numSublist, string relationName, vector<int> &tupleCount,ofstream &out){
    //clog<<"Hash output:"<<numSublist<<" "<<relationName<<endl;
    ifstream sublist;

    string filename = relationName + to_string(numSublist) + ".txt" , tuple;
    sublist.open(filename);
    vector<string> small;
    while(getline(sublist,tuple)){
        if(tuple.length() == 0) break;
        small.push_back(tuple);
    }
    sublist.close();

    // open large relation
    if(relationName == "R"){
        filename = "S" + to_string(numSublist) + ".txt" , tuple;
    }else{
        filename = "R" + to_string(numSublist) + ".txt" , tuple;
    }
    sublist.open(filename);
    //clog<<"Writing to output\n";
    for(int row = 0; row <tupleCount[numSublist];row++){
        //clog<<row<<endl;
        while(getline(sublist,tuple)){
            if(tuple.length() == 0)break;
            //clog<<small[row]<<"::"<<tuple<<endl;
            vector<string> large_tup = splitString(tuple,' ');
            vector<string> small_tup = splitString(small[row],' ');
            if(relationName == "R" && (small_tup[1] == large_tup[0])){
                out<<small_tup[0]<<" "<<small_tup[1]<<" "<<large_tup[1]<<endl;
            }else if(relationName == "S" && (small_tup[0] == large_tup[1])){
                out<<large_tup[0]<<" "<<large_tup[1]<<" "<<small_tup[1]<<endl;
            }
        }
        sublist.clear();
        sublist.seekg(0);
    }
    //clog<<"Hash join ended"<<endl;
    sublist.close();
}

void hashJoin(){

    vector<int> rTupleCount(numBlockInMem,0), sTupleCount(numBlockInMem,0);
    int R_BUCKET = createBuckets(rPath,"R",rTupleCount);
    int S_BUCKET = createBuckets(sPath,"S",sTupleCount);
    
    if(min(R_BUCKET,S_BUCKET) > numBlockInMem*numBlockInMem){
        clog<<"Insufficient Memory Buffers\n";
        exit(-1);
    }

    ofstream out;
    out.open(outputFile);
    for(int buck=0;buck<rTupleCount.size();buck++){
        if(rTupleCount[buck] ==0 || sTupleCount[buck]==0)continue;

        if(rTupleCount[buck] <= sTupleCount[buck]){
            hashOutput(buck,"R",rTupleCount,out);
        }else{
            hashOutput(buck,"S",sTupleCount,out);
        }
    }

    out.close();

}

int main(int argc, char *argv[]){

    rPath = argv[1];
    sPath = argv[2];
    joinType = argv[3]; //sort/hash
    numBlockInMem= atoi(argv[4]);

    //clog<<rPath<<" "<<sPath<<" "<<joinType<<" "<<numBlockInMem<<endl;
    

    validatePath(rPath);
    validatePath(sPath);

    string R_file = getFilename(rPath);
    string S_file = getFilename(sPath);

    // clog<<R_file<<endl;
    // clog<<S_file<<endl;

    outputFile = R_file + '_' + S_file + "_join.txt" ;

    //clog<<"outputFile: "<<outputFile<<endl;


    if(joinType == "sort"){
        sortJoin();
    }else if(joinType == "hash"){
        hashJoin();
    }else{
        cout<<"Invalid join type only sort/hash are valid\n"; 
    }

}
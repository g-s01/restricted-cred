/* libraries required to solve the problem */
#include <map>
#include <set>
#include <array>
#include <ctime>
#include <queue>
#include <bitset>
#include <random>
#include <vector>
#include <string>
#include <cassert>
#include <climits>
#include <complex>
#include <iomanip>
#include <numeric>
#include <iostream>
#include <algorithm>

using namespace std;

/* 
    struct to represent a data chunk, 
    since the size is equal, we don't consider the size of chunk
*/
struct data_chunk{
    /* an id to identify the data chunk */
    int id;
    /* time to process the data chunk */
    int time;
    /* for the purpose of the problem, time = 1 for every chunk */
    data_chunk(int id): id(id), time(1){}
};

/* struct to represent a job */
struct job{
    /* a job is nothing but consists of C_j data chunks */
    vector<data_chunk> C_j;
    /* deadline of the job */
    int deadline;
};

/* global variables related to the problem */
/* maximum number of VMs that can be spawned on a machine */
int S;
/* maximum number of data chunks that can be hosted on a machine */
int B;
/* number of machines */
int N;
/* number of jobs */
int J;
/* set of all data_chunks */
vector<data_chunk> C;
/* number of active machines */
int N_a;
/* frequency of each chunk on jobs */
map<int, int> chunk_map;
/* storing the nodes on which a data chunk is present */
map<int, set<int>> chunk_nodes;
/* storing the total time slots given on a node */
map<int, int> node_time_slots;

/* function to schedule the chunks on machines */
vector<pair<int, int>> schedule(vector<pair<int, int>> c, int nts, int node){
    vector<pair<int, int>> res = c;
    reverse(res.begin(), res.end());
    for(int i = 0; i<c.size(); i++){
        if(c[i].first > nts){
            chunk_nodes[c[i].second].insert(node);
            node_time_slots[node] += nts;
            c[i].first -= nts;
            chunk_map[c[i].second] -= nts;
            nts = 0;
            break;
        }
        else{
            chunk_nodes[c[i].second].insert(node);
            node_time_slots[node] += c[i].first;
            nts -= c[i].first;
            chunk_map[c[i].second] -= c[i].first;
            c[i].first = 0;
            res.pop_back();
        }
    }
    if(res.size()) reverse(res.begin(), res.end());
    return res;
}

/* function to solve the problem for same deadlines */
int cred_s(vector<pair<int, int>> chunks, int deadline){
    if(chunks.size() == 0) return 0;
    sort(chunks.begin(), chunks.end(), greater<pair<int, int>>());
    int max_machines = 0;
    if(chunks.size() <= B){
        max_machines++;
        vector<pair<int, int>> res = schedule(chunks, S*deadline, N_a+max_machines);
        max_machines += cred_s(res, S*deadline);
    }   
    else{
        int sum = 0;
        for(int i = chunks.size()-1; i>=chunks.size()-B; i--){
            sum += chunks[i].first;
        }
        int i = chunks.size()-1, j = chunks.size()-B;
        while(sum < S*deadline && j>=0){
            sum -= chunks[i].first;
            i--; j--;
            sum += chunks[j].first;
        }
        max_machines++;
        vector<pair<int, int>> c, new_chunks;
        for(int k = chunks.size()-1; k>i+1; k--){
            new_chunks.push_back(chunks[k]);
        }
        for(int k = min(i+1, (int)chunks.size()-1); k>=max(j, 0); k--){
            c.push_back(chunks[k]);
        }
        for(int k = j-1; k>=0; k--){
            new_chunks.push_back(chunks[k]);
        }
        vector<pair<int, int>> res = schedule(c, S*deadline, N_a+max_machines);
        if(res.size()) new_chunks.insert(new_chunks.end(), res.begin(), res.end());
        max_machines += cred_s(new_chunks, S*deadline);
    }
    return max_machines;
}

/* function to solve the problem for different deadlines */
void cred_m(vector<job> jobs){
    set<int> deadlines;
    for(auto it: jobs) deadlines.insert(it.deadline);
    for(auto it: deadlines){
        vector<pair<int, int>> chunks;
        for(auto jt: chunk_map){
            chunks.push_back({jt.second, jt.first});
        }
        int temp = cred_s(chunks, it);
        chunks.clear();
        for(int n = 1; n<=temp; n++){
            for(auto jt: deadlines){
                if(jt <= it) continue;
                for(auto kt: chunk_map){
                    if(chunk_nodes[kt.first].count(n) == 0 || kt.second == 0) continue;
                    chunks.push_back({kt.second, kt.first});
                }
                schedule(chunks, S*jt-node_time_slots[n], n);
            }
        }
        N_a += temp;
    }
}

int main(){
    /* initialization */
    N = 2;
    J = 5;
    B = 10;
    S = 2;
    /* creating the data chunks */
    for(int i = 1; i <= 5; i++){
        C.push_back(data_chunk(i));
    }
    /* creating the jobs */
    vector<job> jobs;
    for(int i = 0; i < J; i++){
        job j;
        for(int k = 0; k < 5; k++){
            j.C_j.push_back(C[(i*5 + k)%5+1]);
        }
        j.deadline = rand()%5 + 1;
        jobs.push_back(j);
    }
    /* solving the problem */
    for(auto it: jobs){
        for(auto jt: it.C_j){
            if(chunk_map.find(jt.id) == chunk_map.end()){
                chunk_map[jt.id] = 1;
            }
            else{
                chunk_map[jt.id]++;
            }
        }
    }
    /* testing cred_s */
    vector<pair<int, int>> chunks;
    for(auto it: chunk_map){
        chunks.push_back({it.second, it.first});
    }
    N_a = cred_s(chunks, 5);
    std::cout << "N_a: " << N_a << '\n';
    /* testing cred_m*/
    cred_m(jobs);
    std::cout << "N_a: " << N_a << '\n';
}
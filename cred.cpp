/* libraries required to solve the problem */
#include <map>
#include <set>
#include <array>
#include <ctime>
#include <queue>
#include <bitset>
#include <chrono>
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

mt19937 rng(chrono::steady_clock::now().time_since_epoch().count());

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
/* 
    map for frequency of each chunk on jobs
    key: chunk id 
    value: frequency of the chunk
*/
map<int, int> chunk_map;
/* 
    map for storing the nodes on which a data chunk is present for a particular deadline 
    key: {chunk id, deadline}
    value: set of nodes
*/
map<pair<int, int>, set<int>> chunk_nodes;
/* 
    storing the # of slots given to a data chunk on machine id i for a particular deadline 
    key: {chunk id, deadline}
    value: map of machine id to # of slots
*/
map<pair<int, int>, map<int, int>> chunk_machine_slots;
/* 
    storing the total time slots given on a node 
    key: node id
    value: total time slots left
*/
map<int, int> node_time_slots;
/* 
    ids of the machines which have been created till now 
    key: machine id
*/
set<int> machine_ids;
/* 
    map to store the nodes where a data chunk has been stored
    key: data chunk id
    value: set of nodes
*/
map<int, set<int>> chunk_node_map;

/*
    function to give a random number between integers a and b
    input: a, b
    output: random number between a and b
*/
int between(int a = 1, int b = 100){
    return uniform_int_distribution<int>(a, b)(rng);
}

/* 
    function to schedule the chunks on machines 
    Idea: by scheduling time slots from the chunks with the smallest number of required time slots,
    we can remove more chunks.
    Inputs: 
    c -> vector of chunks in the form of ({chunk freqeuncy, chunk id}), 
    nts -> number of available time slots, 
    node -> node number
    Output:
    vector of chunks that couldn't be scheduled
*/
vector<pair<int, int>> schedule(vector<pair<int, int>> c, int nts, int node, int deadline){
    if(nts == 0) return c;
    cout << "Calling schedule for node " << node << " on modified deadline " << deadline << '\n';
    sort(c.begin(), c.end());
    for(int i = 0; i<c.size(); i++){
        if(chunk_nodes[{c[i].second, deadline}].find(node) != chunk_nodes[{c[i].second, deadline}].end()) continue;
        /* 
            in this case, the data chunk can't be scheduled fully on this node
            so we schedule the data chunk on this node for the minimum of the available time slots 
            and the deadline, exhausting all the available resources on this node
        */
        if(c[i].first > min(node_time_slots[node], deadline)){
            int give = min(node_time_slots[node], deadline);
            cout << "chunk " << c[i].second << " can't be scheduled fully on node " << node << '\n';
            cout << "chunk " << c[i].second << " scheduled on node " << node << " for # of slots " << give << '\n';
            /* we are going to schedule the c[i].second chunk on this node for the current deadline */
            chunk_nodes[{c[i].second, deadline}].insert(node);
            /* we have schedule the c[i].second chunk on this node once */
            chunk_node_map[c[i].second].insert(node);
            /* total time given to c[i].second chunk on this node at this deadline */
            chunk_machine_slots[{c[i].second, deadline}][node] = give;
            /* remaining slots for the node and data chunk */
            node_time_slots[node] -= give;
            chunk_map[c[i].second] -= give;
            nts -= give;
            c[i].first -= give;
            cout << "chunk " << c[i].second << " remaining slots: " << c[i].first << '\n';
            cout << "node " << node << " remaining slots: " << node_time_slots[node] << '\n';
            if(nts == 0) break;
        }
        /* 
            in this case, the data chunk can be scheduled fully on this node
            so we schedule the data chunk on this node for the required time slots 
        */
        else{
            cout << "chunk " << c[i].second << " can be scheduled fully on node " << node << '\n';
            cout << "chunk " << c[i].second << " scheduled on node " << node << " for # of slots " << c[i].first << '\n';
            /* we are going to schedule the c[i].second chunk on this node for the current deadline */
            chunk_nodes[{c[i].second, deadline}].insert(node);
            /* we have schedule the c[i].second chunk on this node once */
            chunk_node_map[c[i].second].insert(node);
            /* total time given to c[i].second chunk on this node at this deadline */
            chunk_machine_slots[{c[i].second, deadline}][node] = c[i].first;
            /* remaining slots for the node and data chunk */
            node_time_slots[node] -= c[i].first;
            nts -= c[i].first;
            chunk_map[c[i].second] = 0;
            c[i].first = 0;
            cout << "node " << node << " remaining slots: " << node_time_slots[node] << '\n';
            if(nts == 0) break;
        }
    }
    vector<pair<int, int>> res;
    for(auto it: c){
        if(it.first != 0) res.push_back(it);
    }
    return res;
}

/* function to solve the problem for same deadlines */
/*
    Idea: find the first set of B chunks which require more than s*deadline time slots, make a new 
    machine and schedule the chunks on it
    then schedule the chunks which require less than s*deadline time slots on the other machines.
    Inputs: 
    chunks -> vector of chunks in the form of ({chunk freqeuncy, chunk id}), 
    virtual_deadline -> virtual deadline of the job
    true_deadline -> true deadline of the job
    Output:
    number of machines required to schedule the chunks
*/
int cred_s(vector<pair<int, int>> chunks, int virtual_deadline, int true_deadline){
    if(chunks.size() == 0) return 0;
    /* sorting the chunk set in decreasing order of reqd computation */
    sort(chunks.begin(), chunks.end(), greater<pair<int, int>>());
    cout << "chunks to be scheduled for deadline " << true_deadline << ": " << '\n';
    for(auto it: chunks){
        cout << it.first << " " << it.second << '\n';
    }
    while(chunks.size()){
        if(chunks.size() <= B){
            /* finding new machine id */
            vector<pair<int, int>> res = chunks;
            /* try to schedule the chunks on previous machines first */
            for(auto jt: machine_ids){
                res = schedule(res, node_time_slots[jt], jt, true_deadline);
            }
            /* try scheduling the chunks on new machine if chunk remain */
            if(res.size()){
                machine_ids.insert(*machine_ids.rbegin()+1);
                node_time_slots[*machine_ids.rbegin()] = S*true_deadline;
                res = schedule(res, node_time_slots[*machine_ids.rbegin()], *machine_ids.rbegin(), true_deadline);
            }
            chunks = res;
        }   
        else{
            /* finding new machine id */
            vector<pair<int, int>> res, new_chunks;
            /* try to schedule the chunks on previous machines first */
            for(auto jt: machine_ids){
                if(chunks.size() <= B){
                    res = schedule(chunks, node_time_slots[jt], jt, true_deadline);
                    if(res.size()) new_chunks.insert(new_chunks.end(), res.begin(), res.end());
                }
                else{
                    int sum = 0;
                    for(int i = chunks.size()-1; i>=chunks.size()-B; i--){
                        sum += chunks[i].first;
                    }
                    int i = chunks.size()-1, j = chunks.size()-B;
                    while(sum < virtual_deadline && j>=0){
                        sum -= chunks[i].first;
                        i--; j--;
                        sum += chunks[j].first;
                    }
                    new_chunks.clear();
                    vector<pair<int, int>> c;
                    for(int k = chunks.size()-1; k>=0; k--){
                        if(j <= k && k <= i){
                            c.push_back(chunks[k]);
                        }
                        else{
                            new_chunks.push_back(chunks[k]);
                        }
                    }
                    cout << "chunk set c: " << '\n';
                    for(auto it: c){
                        cout << it.first << " " << it.second << '\n';
                    }
                    cout << "new chunks set c: " << '\n';
                    for(auto it: new_chunks){
                        cout << it.first << " " << it.second << '\n';
                    }
                    res = schedule(c, node_time_slots[jt], jt, true_deadline);
                    if(res.size()) new_chunks.insert(new_chunks.end(), res.begin(), res.end());
                    cout << "new chunks set c after scheduling: " << '\n';
                    for(auto it: new_chunks){
                        cout << it.first << " " << it.second << '\n';
                    }
                }
                chunks = new_chunks;
                cout << "chunks remaining: " << '\n';
                for(auto it: chunks){
                    cout << it.first << " " << it.second << '\n';
                }
            }
            if(chunks.size()){
                cout << "new machine created: " << *machine_ids.rbegin()+1 << '\n';
                cout << "chunks to be scheduled on machine " << *machine_ids.rbegin()+1 << " for deadline " << true_deadline << ": " << '\n';
                for(auto it: chunks){
                    cout << it.first << " " << it.second << '\n';
                }
                machine_ids.insert(*machine_ids.rbegin()+1);
                node_time_slots[*machine_ids.rbegin()] = S*true_deadline;
                if(chunks.size() <= B){
                    res = schedule(chunks, node_time_slots[*machine_ids.rbegin()], *machine_ids.rbegin(), true_deadline);
                    if(res.size()) new_chunks.insert(new_chunks.end(), res.begin(), res.end());
                }
                else{
                    int sum = 0;
                    for(int i = chunks.size()-1; i>=chunks.size()-B; i--){
                        sum += chunks[i].first;
                    }
                    int i = chunks.size()-1, j = chunks.size()-B;
                    while(sum < virtual_deadline && j>=0){
                        sum -= chunks[i].first;
                        i--; j--;
                        sum += chunks[j].first;
                    }
                    new_chunks.clear();
                    vector<pair<int, int>> c;
                    for(int k = chunks.size()-1; k>=0; k--){
                        if(j <= k && k <= i){
                            c.push_back(chunks[k]);
                        }
                        else{
                            new_chunks.push_back(chunks[k]);
                        }
                    }
                    cout << "chunk set c: " << '\n';
                    for(auto it: c){
                        cout << it.first << " " << it.second << '\n';
                    }
                    cout << "new chunks set c: " << '\n';
                    for(auto it: new_chunks){
                        cout << it.first << " " << it.second << '\n';
                    }
                    res = schedule(c, node_time_slots[*machine_ids.rbegin()], *machine_ids.rbegin(), true_deadline);
                    if(res.size()) new_chunks.insert(new_chunks.end(), res.begin(), res.end());
                    cout << "new chunks set c after scheduling : " << '\n';
                    for(auto it: new_chunks){
                        cout << it.first << " " << it.second << '\n';
                    }
                }
                chunks = new_chunks;
                cout << "chunks remaining: " << '\n';
                for(auto it: chunks){
                    cout << it.first << " " << it.second << '\n';
                }
            }
        }
    }
    return machine_ids.size();
}

/* function to solve the problem for different deadlines */
/*
    Idea: first call cred_s for each deadline, then for each deadline
    call schedule for each node, to reschedule the existing chunks on it to save time in the future
    Inputs:
    jobs -> vector of jobs
*/
void cred_m(vector<job> jobs){
    /* finding the distinct deadlines of jobs */
    set<int> deadlines;
    for(auto it: jobs){
        deadlines.insert(it.deadline);
    }
    /* reqd to calculate modified deadline */
    int prev = 0;
    for(auto it: deadlines){
        /* finding the chunks that need to processed for the given deadline */
        vector<pair<int, int>> chunks;
        map<int, int> temp_chunk_map;
        for(auto jt: jobs){
            if(jt.deadline == it){
                for(auto kt: jt.C_j){
                    temp_chunk_map[kt.id]++;
                }
            }
        }
        for(auto jt: temp_chunk_map){
            chunks.push_back({jt.second, jt.first});
        }
        /* scheduling the given chunks */
        int temp = cred_s(chunks, S*(it-prev), it-prev);
        chunks.clear();
        /* scheduling the already allocated chunks for future on the already allocated machines */
        for(int n = 1; n<=temp; n++){
            int temp_prev = it;
            for(auto jt: deadlines){
                if(jt <= it) continue;
                temp_chunk_map.clear();
                for(auto kt: jobs){
                    if(kt.deadline == jt){
                        for(auto lt: kt.C_j){
                            temp_chunk_map[lt.id]++;
                        }
                    }
                }
                for(auto kt: temp_chunk_map){
                    if(chunk_node_map[kt.first].count(n) == 0 || kt.second == 0) continue;
                    chunks.push_back({kt.second, kt.first});
                }
                schedule(chunks, S*jt-node_time_slots[n], n, jt-temp_prev);
                temp_prev = jt;
            }
        }
        N_a = machine_ids.size();
    }
}

/*
    function for evaluation by simulation
*/
void simulation(){
    /* initialization */
    N = 1;
    J = between(1, 10);
    B = 128;
    S = 4;
    /* creating the data chunks */
    int total_chunks = between(1, 20);
    for(int i = 0; i < total_chunks; i++){
        C.push_back(data_chunk(i));
    }
    /* creating the jobs */
    vector<job> jobs(J);
    for(int i = 0; i<J; i++){
        total_chunks = between(1, 20);
        for(int j = 0; j<total_chunks; j++){
            int x = between(0, C.size()-1);
            jobs[i].C_j.push_back(C[between(0, C.size()-1)]);
        }
        jobs[i].deadline = between(1, 10);
    }
    /* printing the simulation profile */
    cout << "---------------------------- SIMULATION PROFILE ------------------------------" << '\n';
    cout << "Number of jobs: " << J << '\n';
    cout << "Number of data chunks: " << C.size() << '\n';
    cout << "Job profile:" << '\n';
    for(int i = 0; i<J; i++){
        cout << "Job " << i << ": " << '\n';
        cout << "Deadline: " << jobs[i].deadline << '\n';
        cout << "Total data chunks required: " << jobs[i].C_j.size() << '\n';
        cout << "Data chunks: " << '\n';
        for(auto it: jobs[i].C_j){
            cout << it.id << " ";
        }
        cout << '\n';
    } 
    cout << "------------------------------------------------------------------------------" << '\n';
    /* solving the problem */
    for(auto it: jobs){
        for(auto jt: it.C_j){
            chunk_map[jt.id]++;
        }
    }
    machine_ids.insert(0);
    node_time_slots[0] = S*4;
    /* testing cred_m */
    cred_m(jobs);
    std::cout << "N_a: " << N_a << '\n';
}

/*
    function for evaluation by putting manual test cases
*/
void manual(){

}

/* 
    function for evalution using google trace
*/
void trace(){
    cout << "TODO" << '\n';
}

int main(){
    // simulation();
    // manual();
    // trace();
}
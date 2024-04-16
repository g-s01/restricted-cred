# Restricted-CRED: Restricted Cloud Right-sizing with Execution Deadlines and Data Locality

Implementation of the novel-heuristic algorithm proposed in [this](./cred.pdf) paper, with two further constraints:

```
1. In a time slot, only one virtual machine (VM) of a node can access a particular data chunk, not
any other VM can access that chunk
2. The time needed to evaluate a chunk is one unit only, irrespective of the chunk
```

## Running the file

Clone the repository using:

```
git clone https://github.com/g-s01/cred-implementation
```

and run the following commands in the project directory

```
g++ -std=c++17 cred.cpp
./a.out
```

## Footnotes

Done as part of [CS528: High Performance Computing](https://www.iitg.ac.in/cse/course-list.php?id=CS528) course at [IIT Guwahati](https://www.iitg.ac.in/). See the problem statements [here](./ps.pdf).

Course Instructor: [Aryabartta Sahu](https://www.iitg.ac.in/asahu/)

**Credits**
- [Gautam Sharma](https://g-s01.github.io/)
- Adittya Gupta
- Sparsh Mittal

# CS3223 Database Systems Implementation Project
*AY2019/2020 Semester 2*, *School of Computing*, *National University of Singapore*

## Team members
- [Jiang Chen](https://github.com/jcjxwy)
- [Lou Shaw Yeong](https://github.com/xiaoyeong)
- [Ooi Hui Ying](https://github.com/ooihuiying)

## Project Summary
This project involves the implementation of a simple SPJ (select-project-join) query engine to illustrate how query processing works in a modern relational database management system. More details on the project requirement can be found [here.](https://www.comp.nus.edu.sg/~tankl/cs3223/project.html)
The link to our report can be found [here](https://docs.google.com/document/d/1dn1R5YSBkyO6hfUdwYaquXg20MkayHrjba_BgosYExU/edit?usp=sharing).

## Implementation summary
In addition to the given SPJ (Select-Project-Join) query engine, our team implemented the following functions:
1.	**Block Nested Loops join** (See [BlockNestedJoin.java](https://github.com/CS3223-Database-System-Implementation/Database-System-Implementation/blob/7bc64348310745777b1fcd7de2b8c79a2715394f/src/qp/operators/BlockNestedJoin.java))
2.  **SortMerge join** based on ExternalSortMerge and SortedRunComparator (See [SortMerge.java](https://github.com/CS3223-Database-System-Implementation/Database-System-Implementation/blob/master/src/qp/operators/SortMerge.java))
3.  **Distinct** based on ExternalSortMerge and SortedRunComparator (See [Distinct.java](https://github.com/CS3223-Database-System-Implementation/Database-System-Implementation/blob/master/src/qp/operators/Distinct.java))
4.  **Aggregate** functions (MIN, MAX, COUNT, AVG) (See [Aggregate.java](https://github.com/CS3223-Database-System-Implementation/Database-System-Implementation/blob/master/src/qp/operators/Aggregate.java))
5.  Replaced the existing random optimizer with a **greedy heuristics optimizer** (See [GreedyOptimizer.java](https://github.com/CS3223-Database-System-Implementation/Database-System-Implementation/blob/master/src/qp/optimizer/GreedyOptimizer.java))
6.  Identified and fixed the following **bugs/limitations** in the SPJ engine given:
    1. If the query does not involve join, the SPJ does not require the user to **input the number of buffers** (See [QueryMain.java](https://github.com/CS3223-Database-System-Implementation/Database-System-Implementation/blob/master/src/QueryMain.java))
    2. If the input **tuple size is bigger than buffer page size**, SPJ goes to infinity loop (See Open() method of [BlockNestedJoin.java](https://github.com/CS3223-Database-System-Implementation/Database-System-Implementation/blob/7bc64348310745777b1fcd7de2b8c79a2715394f/src/qp/operators/BlockNestedJoin.java), [NestedJoin.java](https://github.com/CS3223-Database-System-Implementation/Database-System-Implementation/blob/7bc64348310745777b1fcd7de2b8c79a2715394f/src/qp/operators/NestedJoin.java), [SortMerge.java](https://github.com/CS3223-Database-System-Implementation/Database-System-Implementation/blob/master/src/qp/operators/SortMerge.java))
    3. If a join query involves **two join conditions on two same tables**, one of the join conditions will be ignored (See [RandomInitialPlan.java](https://github.com/CS3223-Database-System-Implementation/Database-System-Implementation/commit/baeda48e9130ca71a16dc506c5dd218ebc4a9ecb))

## Setup instructions
- Clone this repository into your local repository by doing
    - git clone https://github.com/CS3223-Database-System-Implementation/Database-System-Implementation.git
- Open the project in an IDLE of your choice.
- Make sure you have installed Java not lower than JDK1.8.
- Add the library to the project in the IDLE settings
    - (For eclipse) 
    - In the properties windows, press "Add Folder" to add "src" as a Source Folder from "Java Build Path" -> "Source".
    - In the properties windows, press "Add External Class Folder" to add "lib" from "Java Build Path" -> "Libraries".
    
    
    

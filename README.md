# STRIFE: Handling Highly Contended OLTP Workloads Using Fast Dynamic Partitioning
CMSC624 Database Architecture and implementation final project. Haoran Zhou, Tianrui Guan, Junran Yang

# Reference
https://dl.acm.org/doi/pdf/10.1145/3318464.3389764

# Missing LockManager, TxnProcessor implementation
We deleted those parts because they are part of the assigment of the CMSC624 course https://github.com/abadid/624Assignment2.

# Known bugs
The serial clusterer can work fine but there are known memory corruption issues about the parallel clusterer.

# Future work
We decided not to work on our work further at least for 

# For developers
Branching: create ur own branch from develop, merge into and merge from the develop branch frequently to prevent diverging too much from the develop branch.

Adding source file: when adding a source file, we need to add the xxx.cc file in the Makefile.inc in the corresponding subdir.

Adding tests: It's better to keep your unit tests in the xxx_test.cc file. Please don't write private tests and then just drop them once they are passed. In order to run the tests, we need to first have a xxx.cc file and add it to SUB_DIR_NAME_SRC and then name the test file as xxx_test.cc.

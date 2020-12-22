OBJDIRS += txn

# Create template specific variables
UPPERC_DIR := TXN
LOWERC_DIR := txn

TXN_SRCS := txn/storage.cc txn/txn_types.cc txn/mvcc_storage.cc txn/txn.cc txn/lock_manager.cc txn/txn_processor.cc txn/clusterer.cc txn/union_find.cc txn/printer.cc txn/clustere_loadgen.cc

SRC_LINKED_OBJECTS :=
TEST_LINKED_OBJECTS :=

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Need to specify test cases explicitly because they have variables in recipe
test-txn: $(TXN_TESTS)
	@for a in $(TXN_TESTS); do \
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done

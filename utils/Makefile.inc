OBJDIRS += utils

# Create template specific variables
UPPERC_DIR := UTILS
LOWERC_DIR := utils

UTILS_SRCS := utils/mutex.cc

SRC_LINKED_OBJECTS :=
TEST_LINKED_OBJECTS :=

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Need to specify test cases explicitly because they have variables in recipe
test-utils: $(UTILS_TESTS)
	@for a in $(UTILS_TESTS); do \
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done

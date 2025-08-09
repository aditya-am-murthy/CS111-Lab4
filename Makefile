ifeq ($(shell uname -s),Darwin)
	CFLAGS = -std=gnu17 -pthread -Wall -O0 -pipe -fno-plt -fPIC -I. -I/opt/homebrew/include
	LDFLAGS = -pthread -L$(shell brew --prefix)/lib -largp
else
	CFLAGS = -std=gnu17 -pthread -Wall -O0 -pipe -fno-plt -fPIC -I.
	LDFLAGS = -lrt -pthread -Wl,-O1,--sort-common,--as-needed,-z,relro,-z,now
endif


OBJS = \
  hash-table-common.o \
  hash-table-base.o \
  hash-table-v1.o \
  hash-table-v2.o \
  hash-table-tester.o

GRADED_OBJS = \
  hash-table-common.o \
  hash-table-base.o \
  hash-table-v1.o \
  hash-table-v2.o \
  hash-table-tester-graded.o

.PHONY: all
all: hash-table-tester

hash-table-tester: $(OBJS)
	$(CC) $(LDFLAGS) $^ -o $@

.PHONY: graded
graded: tester-graded

tester-graded: $(GRADED_OBJS)
	$(CC) $(LDFLAGS) $^ -o $@

.PHONY: clean
clean:
	rm -f $(OBJS) $(GRADED_OBJS) hash-table-tester tester-graded

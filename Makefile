CC=$(CROSS_COMPILE)gcc

CFLAGS+=-Wall -std=gnu11 -fPIC -D_LARGEFILE64_SOURCE
LDFLAGS+=
INCLUDE +=

.PHONY: all

all: dnd

.PHONY: debug

debug: CFLAGS += -O0 -DDEBUG -g -Wno-unused-variable
debug: all

dnd: dnd.o
	$(CC) $(CFLAGS) -o dnd dnd.o $(LDFLAGS)

dnd.o : dnd.c
	$(CC) $(CFLAGS) $(VERSION) -c dnd.c $(INCLUDE)

clean:
	-rm dnd
	-rm dnd.o

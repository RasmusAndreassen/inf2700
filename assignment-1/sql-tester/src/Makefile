CC = gcc
INCLUDES =
LDFLAGS = -lsqlite3
CFLAGS = -g -Wall

TARGET = testsql
OBJ_DIR = ../_obj
TEST_DIR = ../tests
HEADERS =
OBJS =

# Main target
all: $(TARGET)

testsql: testsql.c
	$(CC) $(CFLAGS) -o ../$@ $< $(LDFLAGS)

.PHONY: clean
clean:
	rm -f ../testsql

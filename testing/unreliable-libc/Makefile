CC = gcc
CFLAGS = -Wall -Wextra -fPIC
LDFLAGS = -shared
TARGET = unreliable-libc.so
SRCS = file.c
OBJS = $(SRCS:.c=.o)

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ -ldl

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

install: $(TARGET)
	install -m 755 $(TARGET) /usr/local/lib

uninstall:
	rm -f /usr/local/lib/$(TARGET)

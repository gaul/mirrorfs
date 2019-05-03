CFLAGS = -g -Wall -Wextra -Wno-unused-function -Wno-unused-parameter -Werror

mirrorfs: mirrorfs.c
	$(CC) $(CFLAGS) mirrorfs.c `pkg-config fuse3 --cflags --libs` -D_FILE_OFFSET_BITS=64 -o mirrorfs

clean:
	rm -f mirrorfs

all: mirrorfs

test: all
	./test.sh

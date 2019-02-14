CC = gcc
PROM = stackfs
SOURCE = fs_main.c fs/fs.c
$(PROM) : $(SOURCE)
	$(CC) -o $(PROM) $(SOURCE) `pkg-config fuse --cflags --libs`
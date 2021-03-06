#define FUSE_USE_VERSION 30
#include <stdio.h>
#include <string.h>
#include <fuse.h>
#include <malloc.h>

#include "fs/fs.h"


int fuse_open(const char *path, struct fuse_file_info *fileInfo)
{
	return fs_open(path, fileInfo);
}

int fuse_create(const char * path, mode_t mode, struct fuse_file_info * info)
{
    return fs_create(path, mode, info);
}

int fuse_mkdir(const char *path, mode_t mode)
{
	return fs_mkdir(path, mode);
}

int fuse_opendir(const char *path, struct fuse_file_info *fileInfo)
{
    return fs_opendir(path, fileInfo);
}

int fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fileInfo)
{
	return fs_readdir(path, buf, filler, offset, fileInfo);
}

int fuse_getattr(const char* path, struct stat* st)
{
    return fs_getattr(path, st);
}

int fuse_rmdir(const char *path)
{
	return fs_rmdir(path);
}

int fuse_rename(const char *path, const char *newpath)
{
	return fs_rename(path, newpath);
}

int fuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo)
{
	return fs_read(path, buf, size, offset, fileInfo);
}

int fuse_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo)
{
	return fs_write(path, buf, size, offset, fileInfo);
}

int fuse_release(const char *path, struct fuse_file_info *fileInfo)
{
    return fs_release(path, fileInfo);
}

int fuse_releasedir(const char *path, struct fuse_file_info *fileInfo)
{
	return fs_releasedir(path, fileInfo);
}

int fuse_utimens(const char * path, const struct timespec tv[2])
{
	return fs_utimens(path, tv);
}

int fuse_truncate(const char * path, off_t offset)
{
	return fs_truncate(path, offset);
}

int fuse_unlink(const char * path)
{
	return fs_unlink(path);
}

int fuse_chmod(const char * path, mode_t mode)
{
	return fs_chmod(path, mode);
}

int fuse_chown(const char * path, uid_t owner, gid_t group)
{
	return fs_chown(path, owner, group);
}

int fuse_access(const char * path, int amode)
{
	return fs_access(path, amode);
}

int fuse_symlink(const char * oldpath, const char * newpath)
{
	return fs_symlink(oldpath, newpath);
}

int fuse_readlink(const char * path, char * buf, size_t size)
{
	return fs_readlink(path, buf, size);
}

int fuse_statfs(const char *path, struct statvfs *statv)
{
	return fs_statfs(path, statv);
}

static struct fuse_operations fuse_ops =
{
    .open = fuse_open,
    .mkdir = fuse_mkdir,
    .opendir = fuse_opendir,
    .readdir = fuse_readdir,
    .getattr = fuse_getattr,
    .rmdir = fuse_rmdir,
    .rename = fuse_rename,
    .create = fuse_create,
    .read = fuse_read,
    .write = fuse_write,
    .release = fuse_release,
    .releasedir = fuse_releasedir,
    .utimens = fuse_utimens,
    .truncate = fuse_truncate,
    .unlink = fuse_unlink,
    .chmod = fuse_chmod,
    .chown = fuse_chown,
    .access = fuse_access,
    .symlink = fuse_symlink,
    .readlink = fuse_readlink,
    .statfs = fuse_statfs,
};

static void usage(void)
{
    printf(
    "usage:./stackfs /mnt/mountpoint /mnt/access -d\n"
    );
}

int main(int argc, char * argv[])
{
	int ret;
	int i;

	for (i = 0; i < argc; i++) {
		if ((argc < 3) || (strcmp(argv[i], "--help") == 0)) {
			usage();
			return 0;
		}
	}

	int fuse_argc = 0;
	//char * fuse_argv[argc];
	char * fuse_argv[20];
	fuse_argv[fuse_argc++] = argv[0];
	fuse_argv[fuse_argc++] = argv[1];    // mount point
	if (argc > 3) {
		fuse_argv[fuse_argc++] = argv[3];
	}
	/*
	for (i = 0; i < argc; i++) {
		if (argv[i] != NULL) {
			fuse_argv[fuse_argc++] = argv[i];
		}
	}
	*/

	fs_init(argv[1], argv[2]);
	printf("starting fuse main...\n");
	ret = fuse_main(fuse_argc, fuse_argv, &fuse_ops, NULL);
	printf("fuse main finished, ret %d\n", ret);
	fs_destroy();
	return ret;
}
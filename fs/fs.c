#include "fs.h"

void fs_init(char * mount_point, char * access_point)
{

}

int fs_open(const char *path, struct fuse_file_info *fileInfo)
{

}

int fs_create(const char * path, mode_t mode, struct fuse_file_info * info)
{

}

int fs_mkdir(const char *path, mode_t mode)
{

}

int fs_opendir(const char *path, struct fuse_file_info *fileInfo)
{

}

int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fileInfo)
{

}

int fs_getattr(const char* path, struct stat* st)
{

}

int fs_rmdir(const char *path)
{

}

int fs_rename(const char *path, const char *newpath)
{

}

int fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo)
{

}

int fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo)
{

}

int fs_release(const char *path, struct fuse_file_info *fileInfo)
{

}

int fs_releasedir(const char * path, struct fuse_file_info * info)
{

}

int fs_utimens(const char * path, const struct timespec tv[2])
{

}

int fs_truncate(const char * path, off_t length)
{

}

int fs_unlink(const char * path)
{

}

int fs_chmod(const char * path, mode_t mode)
{

}

int fs_chown(const char * path, uid_t owner, gid_t group)
{

}

int fs_access(const char * path, int amode)
{

}

int fs_symlink(const char * oldpath, const char * newpath)
{

}

int fs_readlink(const char * path, char * buf, size_t size)
{

}

int fs_destroy()
{

}
#ifndef FS_H
#define FS_H

#define FUSE_USE_VERSION 30
#include <fuse.h>
#include <string.h>
#include <pthread.h>
#include <sys/statvfs.h>
#include "../tools/map.h"

#define DIR_DENTRY 0
#define FILE_DENTRY 1
#define SMALL_FILE 1
#define NORMAL_FILE 0

#define PRE_LOC_NUM 1000
#define EACH_SUBDIR 13
#define MAX_COUNT_LIMIT 1010
#define PATH_LEN 225
#define DENTRY_NAME_SIZE 128
#define ALLOCATED_PATH "pre_alloc"

// map config
#define MAP_KEY_DELIMIT "#"
#define MAP_KEY_LEN 128
#define MAP_PRE_KEY_LEN 10    // inode +'#'


#define ERROR -1
#define SUCCESS 0
// lookup error code
#define LOOKUP_SUCCESS 0
#define MISS_FILE 1
#define MISS_DIR 2

#define REALLOC_ENABLE true

// for DEBUG
#define FS_DEBUG

#define likely(x)    __builtin_expect(!!(x), 1)
#define unlikely(x)  __builtin_expect(!!(x), 0)


struct dentry {
	uint32_t fid;    // name in lustre
	uint32_t inode;
	uint32_t flags;
	uint32_t mode;
	uint32_t ctime;
	uint32_t mtime;
	uint32_t atime;
	uint32_t size;
	uint32_t uid;
	uint32_t gid;
	uint32_t nlink;
};

struct dirty_dentry {
	struct dentry *dentry;
	struct dirty_dentry *prev;
	struct dirty_dentry *next;
};

struct unused_dentry {
	struct dentry *dentry;
	struct unused_dentry *prev;
	struct unused_dentry *next;
};

struct fs_super {
	char alloc_path[PATH_LEN];
	char mount_point[PATH_LEN];
	
	struct dirty_dentry *dirty_dentry_head;
	struct dirty_dentry *dirty_dentry_tail;
	struct unused_dentry *unused_dentry_head;
	struct unused_dentry *unused_dentry_tail;
	root_t tree;
	root_t link_tree;
	uint32_t curr_dir_id;
	pthread_mutex_t dir_id_lock;
	pthread_rwlock_t dirty_list_rwlock;
	pthread_rwlock_t unused_list_rwlock;
	pthread_rwlock_t tree_rwlock;
	pthread_rwlock_t link_tree_rwlock;
};

enum dentryflags {
	D_type,    // file/dir
	D_small_file,    // 1 is small file, 0 is normal file
	D_dirty,
};

struct lookup_res {
	struct dentry *dentry;
	int p_inode;
	int error;
};


uint32_t generate_unique_id();
void set_dentry_flag(struct dentry *dentry, int flag_type, int val);
int get_dentry_flag(struct dentry *dentry, int flag_type);
int add_dentry_to_dirty_list(struct dentry *dentry);
int remove_dentry_from_dirty_list(struct dentry *dentry);
int add_dentry_to_unused_list(struct dentry *dentry);
int remove_dentry_from_unused_list(struct dentry *dentry);
int charlen(char *str);
void init_sb(char * mount_point, char * access_point);
int path_lookup(const char *path, struct lookup_res *lkup_res);
void batch_realloc();

// operation interface api
void fs_init(char * mount_point, char * access_point);

int fs_open(const char *path, struct fuse_file_info *fileInfo);

int fs_create(const char * path, mode_t mode, struct fuse_file_info * info);

int fs_mkdir(const char *path, mode_t mode);

int fs_opendir(const char *path, struct fuse_file_info *fileInfo);

int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fileInfo);

int fs_getattr(const char* path, struct stat* st);

int fs_rmdir(const char *path);

int fs_rename(const char *path, const char *newpath);

int fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo);

int fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo);

int fs_release(const char *path, struct fuse_file_info *fileInfo);

int fs_releasedir(const char * path, struct fuse_file_info * info);

int fs_utimens(const char * path, const struct timespec tv[2]);

int fs_truncate(const char * path, off_t length);

int fs_unlink(const char * path);

int fs_chmod(const char * path, mode_t mode);

int fs_chown(const char * path, uid_t owner, gid_t group);

int fs_access(const char * path, int amode);

int fs_symlink(const char * oldpath, const char * newpath);

int fs_readlink(const char * path, char * buf, size_t size);

int fs_statfs(const char *path, struct statvfs *statv);

int fs_destroy();

#endif
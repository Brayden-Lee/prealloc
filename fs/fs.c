#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <libgen.h>

#include "fs.h"

struct fs_super *fs_sb = NULL;


void set_dentry_flag(struct dentry *dentry, int flag_type, int val)
{
	if (val == 0)
	{
		// set 0
		dentry->flags &= ~(1UL << flag_type);
	} else {
		// set 1
		dentry->flags |= 1UL << flag_type;
	}
}

int get_dentry_flag(struct dentry *dentry, int flag_type)
{
	if (((dentry->flags >> flag_type) & 1) == 1 )
	{
		return 1;
	} else {
		return 0;
	}
}

int add_dentry_to_dirty_list(struct dentry *dentry)
{
	set_dentry_flag(dentry, D_dirty, 1);
	struct dirty_dentry *dirty_dentry = (struct dirty_dentry *) calloc(1, sizeof(struct dirty_dentry));
	dirty_dentry->dentry = dentry;
	dirty_dentry->prev = NULL;
	dirty_dentry->next = NULL;

	if (fs_sb->dirty_dentry_head == NULL) {
		fs_sb->dirty_dentry_head = dirty_dentry;
		fs_sb->dirty_dentry_tail = dirty_dentry;
		return 0;
	}

	dirty_dentry->next = fs_sb->dirty_dentry_head;
	fs_sb->dirty_dentry_head->prev = dirty_dentry;
	fs_sb->dirty_dentry_head = dirty_dentry;
	return 0;
}

// only remove from tail
int remove_dentry_from_dirty_list(struct dentry *dentry)
{
	struct dirty_dentry * dirty_dentry = fs_sb->dirty_dentry_tail;
	while (dirty_dentry->dentry->fid != dentry->fid && dirty_dentry != NULL) {
		dirty_dentry = dirty_dentry->prev;
	}
	if (dirty_dentry == NULL) {
		printf("this dentry not in dirty list\n");
		return 0;
	}
	if (dirty_dentry == fs_sb->dirty_dentry_tail) {
		if (dirty_dentry->prev == NULL) {
			fs_sb->dirty_dentry_head = NULL;
			fs_sb->dirty_dentry_tail = NULL;
			goto out;
		}
		fs_sb->dirty_dentry_tail = dirty_dentry->prev;
		dirty_dentry->prev->next = NULL;
	} else {
		dirty_dentry->next->prev = dirty_dentry->prev;
		if (dirty_dentry->prev != NULL) {
			dirty_dentry->prev->next = dirty_dentry->next;
		} else {
			fs_sb->dirty_dentry_head = dirty_dentry->next;
		}
	}
out:
	set_dentry_flag(dentry, D_dirty, 0);
	free(dirty_dentry);
	dirty_dentry = NULL;
	return 0;	
}

// front insert
int add_dentry_to_unused_list(struct dentry *dentry)
{
	set_dentry_flag(dentry, D_dirty, 0);
	struct unused_dentry *unused_dentry = (struct unused_dentry *) calloc(1, sizeof(struct unused_dentry));
	unused_dentry->dentry = dentry;
	unused_dentry->prev = NULL;
	unused_dentry->next = NULL;

	if (fs_sb->unused_dentry_head == NULL) {
		fs_sb->unused_dentry_head = unused_dentry;
		fs_sb->unused_dentry_tail = unused_dentry;
		return 0;
	}

	unused_dentry->next = fs_sb->unused_dentry_head;
	fs_sb->unused_dentry_head->prev = unused_dentry;
	fs_sb->unused_dentry_head = unused_dentry;
	return 0;
}

// only remove from tail
int remove_dentry_from_unused_list(struct dentry *dentry)
{
	struct unused_dentry * unused_dentry = fs_sb->unused_dentry_tail;
	while (unused_dentry->dentry->fid != dentry->fid && unused_dentry != NULL) {
		unused_dentry = unused_dentry->prev;
	}
	if (unused_dentry == NULL) {
		printf("this dentry not in dirty list\n");
		return 0;
	}
	if (unused_dentry == fs_sb->unused_dentry_tail) {
		if (unused_dentry->prev == NULL) {
			fs_sb->unused_dentry_head = NULL;
			fs_sb->unused_dentry_tail = NULL;
			goto out;
		}
		fs_sb->unused_dentry_tail = unused_dentry->prev;
		unused_dentry->prev->next = NULL;
	} else {
		unused_dentry->next->prev = unused_dentry->prev;
		if (unused_dentry->prev != NULL) {
			unused_dentry->prev->next = unused_dentry->next;
		} else {
			fs_sb->unused_dentry_head = unused_dentry->next;
		}
	}
out:
	set_dentry_flag(dentry, D_dirty, 1);
	free(unused_dentry);
	unused_dentry = NULL;
	return 0;	
}

int charlen(char *str)
{
	int len = 0;
	while (str[len] != '\0') {
		len++;
	}
	return len;
}


void init_sb(char * mount_point, char * access_point)
{
	fs_sb = (struct fs_super *) calloc(1, sizeof(struct fs_super));
	strcpy(fs_sb->alloc_path, access_point);
	fs_sb->dirty_dentry_head = NULL;
	fs_sb->dirty_dentry_tail = NULL;
	fs_sb->unused_dentry_head = NULL;
	fs_sb->unused_dentry_tail = NULL;
}

void fs_init(char * mount_point, char * access_point)
{
	init_sb(mount_point, access_point);

	char create_path[PATH_LEN];
	memset(create_path, 0, PATH_LEN);
	strcpy(create_path, fs_sb->alloc_path);
	strcat(create_path, "/");
	strcat(create_path, ALLOCATED_PATH);    // // like /mnt/lustre/pre_alloc
	create_path[charlen(create_path)] = '\0';
	
	uint32_t i = 0;
	char part[8];
	int fd = 0;
	struct stat buf;
	struct dentry *dentry = (struct dentry *) calloc(1, sizeof(struct dentry));
	
	for (i = 0; i < PRE_LOC_NUM; i++) {
		sprintf(part, "%d", i);
		strcat(create_path, "/");
		strcat(create_path, part);
		create_path[charlen(create_path)] = '\0';
		fd = open(create_path, O_CREAT);
		if (unlikely(fd)) {
			printf("Init... part = %s not be created\n", part);
		}
		stat(create_path, &buf);
		// hook dentry
		dentry->fid = i;    // name in lustre
		dentry->inode = buf.st_ino;
		dentry->flags = 0;
		dentry->mode = buf.st_mode;
		dentry->ctime = buf.st_ctime;
		dentry->mtime = buf.st_mtime;
		dentry->atime = buf.st_atime;
		dentry->size = buf.st_size;
		dentry->uid = buf.st_uid;
		dentry->gid = buf.st_gid;
		dentry->nlink = buf.st_nlink;

		add_dentry_to_unused_list(dentry);

		// prealloc for next
		dirname(create_path);
	}
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
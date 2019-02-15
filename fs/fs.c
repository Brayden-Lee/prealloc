#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <stdbool.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <libgen.h>

#include "fs.h"
#include "../tools/rbtree.h"

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

struct dentry* fetch_dentry_from_unused_list()
{
	if (fs_sb->unused_dentry_tail == NULL)    // not enough
		return NULL;
	struct unused_dentry *fetched_dentry = NULL;
	fetched_dentry = fs_sb->unused_dentry_tail;
	fs_sb->unused_dentry_tail = fetched_dentry->prev;
	fetched_dentry->prev = NULL;
	return fetched_dentry->dentry;
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
	fs_sb->tree = RB_ROOT;

	// root should in map first!
	struct stat root_buf;
	stat(access_point, &root_buf);
	struct dentry *dentry = (struct dentry *) calloc(1, sizeof(struct dentry));
	dentry->fid = 0;    // name in lustre
	dentry->inode = root_buf.st_ino;
	dentry->flags = 0;
	set_dentry_flag(dentry, D_type, DIR_DENTRY);
	dentry->mode = root_buf.st_mode;
	dentry->ctime = root_buf.st_ctime;
	dentry->mtime = root_buf.st_mtime;
	dentry->atime = root_buf.st_atime;
	dentry->size = root_buf.st_size;
	dentry->uid = root_buf.st_uid;
	dentry->gid = root_buf.st_gid;
	dentry->nlink = root_buf.st_nlink;
	add_dentry_to_dirty_list(dentry);

	// put in map
	uint64_t addr = (uint64_t) dentry;
	put(&(fs_sb->tree), "0#/", addr);
}

int path_lookup(const char *path, struct lookup_res *lkup_res)
{
	int s, len = strlen(path);
	int cur_inode = 0;
	uint64_t addr = 0;
	map_t *map_item = NULL;
	char *dentry_name = NULL;
	struct dentry *find_dentry = NULL;
	dentry_name = (char *) calloc(1, len + 1);
	char search_key[MAP_KEY_LEN];
	memset(search_key, 0, MAP_KEY_LEN);

	s = 0;
	while (s <= len) {
		if (s == len || path[s] == '/') {
			s = s ? s : 1;
			memcpy(dentry_name, path, s);
			dentry_name[s] = '\0';
			sprintf(search_key, "%d", cur_inode);
			strcat(search_key, MAP_KEY_DELIMIT);
			strcat(search_key, dentry_name);
			search_key[charlen(search_key)] = '\0';
			map_item = get(&(fs_sb->tree), search_key);
			if (map_item == NULL) {
				lkup_res->dentry = NULL;
				if (s == len) {
					lkup_res->error = MISS_FILE;
				} else {
					lkup_res->error = MISS_DIR;
				}
				return ERROR;
			}
			addr = map_item->val;
			find_dentry = (struct dentry *) addr;
			cur_inode = (int) find_dentry->inode;
		}
		s++;
	}
	lkup_res->dentry = find_dentry;
	lkup_res->error = LOOKUP_SUCCESS;
	return SUCCESS;
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
	
	for (i = 1; i < PRE_LOC_NUM; i++) {
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

int fs_create(const char * path, mode_t mode, struct fuse_file_info * fileInfo)
{
	int ret = 0;
	int len = strlen(path);
	int i;
	for (i = len - 1; i >= 0; --i) {
		if (path[i] == '/')
			break;
	}
	len = (i > 0) ? i : 1;
	char *p_path = (char *)calloc(1, len + 1);
	if (i < 0)
		return -ENOENT;
	int j;
	for (j = 0; j < len; ++j) {
		p_path[j] = path[j];
	}
	p_path[j] = '\0';
	char cur_name[DENTRY_NAME_SIZE];
	for (i = len, j = 0; i < strlen(path); i++, j++) {
		cur_name[j] = path[i];
	}
	cur_name[j] = '\0';

	if (fileInfo != NULL)
		fileInfo->flags |= O_CREAT;
	struct lookup_res *lkup_res = NULL;
	lkup_res = (struct lookup_res *)malloc(sizeof(struct lookup_res));
	ret = path_lookup(p_path, lkup_res);
	if (ret == ERROR) {
		ret = -ENOENT;
		goto out;
	}
	if (get_dentry_flag(lkup_res->dentry, D_type) != DIR_DENTRY) {
		ret = -ENOTDIR;
		goto out;
	}
	struct dentry *create_dentry = NULL;
	create_dentry = fetch_dentry_from_unused_list();
	if (create_dentry == NULL) {
		ret = -ENFILE;    // not enough, need pre-alloc
		goto out;
	}
	add_dentry_to_dirty_list(create_dentry);
	set_dentry_flag(create_dentry, D_type, FILE_DENTRY);
	char create_key[MAP_KEY_LEN];
	sprintf(create_key, "%d", (int)create_dentry->inode);
	strcat(create_key, MAP_KEY_DELIMIT);
	strcat(create_key, cur_name);
	create_key[charlen(create_key)] = '\0';
	uint64_t addr = (uint64_t) create_dentry;
	put(&(fs_sb->tree), create_key, addr);

	ret = SUCCESS;
	if (fileInfo != NULL)
		fileInfo->fh = 0;
out:
	free(lkup_res);
	lkup_res = NULL;
	return ret;
}

int fs_mkdir(const char *path, mode_t mode)
{
	
}

int fs_opendir(const char *path, struct fuse_file_info *fileInfo)
{
	int ret = 0;
	struct lookup_res *lkup_res = NULL;
	lkup_res = (struct lookup_res *)malloc(sizeof(struct lookup_res));
	ret = path_lookup(path, lkup_res);
	if (ret == ERROR) {
		ret = -ENOENT;
		goto out;
	} else {
		if (get_dentry_flag(lkup_res->dentry, D_type) != DIR_DENTRY) {
			ret = -ENOTDIR;
			goto out;
		}
		fileInfo->fh = (uint64_t) lkup_res->dentry;
	}
out:
	free(lkup_res);
	lkup_res = NULL;
	return ret;
}

int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fileInfo)
{
	int i = 0;
	int j = 0;
	uint64_t addr = fileInfo->fh;
	struct dentry *p_dentry = NULL;
	p_dentry = (struct dentry *) addr;
	if (p_dentry == NULL)
		return ERROR;

	if (filler(buf, ".", NULL, 0) < 0) {
		printf("filler . error in func = %s\n", __FUNCTION__);
		return ERROR;
	}
	if (filler(buf, "..", NULL, 0) < 0) {
		printf("filler . error in func = %s\n", __FUNCTION__);
		return ERROR;
	}

	char matched_name[DENTRY_NAME_SIZE];
	char pre_key[MAP_PRE_KEY_LEN];
	memset(matched_name, 0, DENTRY_NAME_SIZE);
	memset(pre_key, 0, MAP_PRE_KEY_LEN);
	sprintf(pre_key, "%d", (int)p_dentry->inode);
	strcat(pre_key, MAP_KEY_DELIMIT);
	int len = charlen(pre_key);
	pre_key[len] = '\0';

	map_t *node;
	char *key = NULL;
	bool match = true;
	for (node = map_first(&(fs_sb->tree)); node; node = map_next(&(node->node))) {
		key = node->key;
		for (i = 0; i < len; i++) {
			if (pre_key[i] != key[i]) {
				match = false;
				break;
			}
		}
		if (match) {
			for (i = len, j = 0; i < strlen(key); i++, j++) {
				matched_name[j] = key[i];
			}
			matched_name[j] = '\0';
			if (filler(buf, matched_name, NULL, 0) < 0) {
				printf("filler %s error in func = %s\n", matched_name, __FUNCTION__);
				return ERROR;
			}
		} else {
			match = true;
		}
	}
	return SUCCESS;
}

int fs_getattr(const char* path, struct stat* st)
{
	int ret = 0;
	struct dentry *dentry = NULL;
	struct lookup_res *lkup_res = NULL;
	lkup_res = (struct lookup_res *)malloc(sizeof(struct lookup_res));
	ret = path_lookup(path, lkup_res);
	if (ret == ERROR) {
		ret = -ENOENT;
		goto out;
	}
	dentry = lkup_res->dentry;
	if (strcmp(path, "/") == 0) {
		st->st_mode = S_IFDIR | 0755;
	} else {
		st->st_mode = dentry->mode;
	}
	// copy some parements from dentry to st
	st->st_nlink = dentry->nlink;
	st->st_size = dentry->size;
	st->st_ctime = dentry->ctime;

	st->st_uid = dentry->uid;
	st->st_gid = dentry->gid;
	st->st_atime = dentry->atime;
	st->st_mtime = dentry->mtime;
	dentry->atime = time(NULL);
out:
	free(lkup_res);
	lkup_res = NULL;
	return ret;
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
	info->fh = -1;
	return SUCCESS;
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
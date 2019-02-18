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

uint32_t generate_unique_id()
{
	fs_sb->curr_dir_id++;
	return fs_sb->curr_dir_id;
}

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
	fs_sb->curr_dir_id = 1;

	// root should in map first!
	struct stat root_buf;
	stat(access_point, &root_buf);
	struct dentry *dentry = (struct dentry *) calloc(1, sizeof(struct dentry));
	dentry->fid = 0;    // name in lustre
	//dentry->inode = root_buf.st_ino;
	//dentry->inode = generate_unique_id();
	dentry->inode = 1;    // root inode;
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
	int last_pos = 0;
	int cur_inode = 0;
	uint64_t addr = 0;
	map_t *map_item = NULL;
	struct dentry *find_dentry = NULL;
	char dentry_name[DENTRY_NAME_SIZE];
	char search_key[MAP_KEY_LEN];
	

	s = 0;
	last_pos = 0;
	while (s <= len) {
		if (s == len || path[s] == '/') {
			memset(dentry_name, '\0', MAP_KEY_LEN);
			memset(search_key, '\0', MAP_KEY_LEN);
			s = s ? s : 1;
			memcpy(dentry_name, &path[last_pos], s - last_pos);
			sprintf(search_key, "%d", cur_inode);
			strcat(search_key, MAP_KEY_DELIMIT);
			strcat(search_key, dentry_name);
		#ifdef FS_DEBUG
			printf("path_lookup, dentry name = %s, search key = %s\n", dentry_name, search_key);
		#endif
			map_item = get(&(fs_sb->tree), search_key);
			if (map_item == NULL) {

			#ifdef FS_DEBUG
				printf("path_lookup, not find key = %s item in the map\n", search_key);
			#endif

				lkup_res->dentry = find_dentry;    // if failed record the last searched dentry
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
			if (s == 1)
				last_pos = s;
			else
				last_pos = s + 1;
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
	memset(create_path, '\0', PATH_LEN);
	strcpy(create_path, fs_sb->alloc_path);
	strcat(create_path, "/");
	strcat(create_path, ALLOCATED_PATH);    // // like /mnt/lustre/pre_alloc

	if (access(create_path, F_OK) != 0) {
		mkdir(create_path, O_CREAT);
	}
	
	uint32_t i = 0;
	char part[8];
	int fd = 0;
	struct stat buf;
	struct dentry *dentry = (struct dentry *) calloc(1, sizeof(struct dentry));
	
	for (i = 1; i < PRE_LOC_NUM; i++) {
		memset(part, '\0', 8);
		sprintf(part, "%d", i);
		strcat(create_path, "/");
		strcat(create_path, part);
		create_path[charlen(create_path)] = '\0';
		fd = open(create_path, O_CREAT);
	#ifdef FS_DEBUG
		printf("fs_init, create_path = %s, open fd = %d\n", create_path, fd);
	#endif
		if (unlikely(fd < 0)) {
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
		close(fd);
	}
}

int fs_open(const char *path, struct fuse_file_info *fileInfo)
{
	int ret = 0;
	struct dentry *dentry = NULL;
	struct lookup_res *lkup_res = NULL;
	lkup_res = (struct lookup_res *) malloc(sizeof(struct lookup_res));
	ret = path_lookup(path, lkup_res);
	if (lkup_res->error == MISS_DIR) {
		ret = -ENOENT;
		goto out;
	}
	dentry = lkup_res->dentry;
	if (lkup_res->error == MISS_FILE) {
		if ((fileInfo->flags & O_CREAT) == 0) {
			ret = -ENOENT;
			goto out;
		}
		if (get_dentry_flag(dentry, D_type) != DIR_DENTRY) {
			ret = -ENOTDIR;
			goto out;
		}
		char cur_name[DENTRY_NAME_SIZE];
		int len = strlen(path);
		int i = 0;
		int j = 0;
		for (i = len - 1; i >= 0; --i) {
			if (path[i] == '/')
				break;
		}
		for (i = i + 1, j = 0; i < len; i++, j++) {
			cur_name[j] = path[i];
		}
		cur_name[j] = '\0';

		uint32_t p_inode = dentry->inode;
		struct dentry *create_dentry = NULL;
		create_dentry = fetch_dentry_from_unused_list();
		if (create_dentry == NULL) {
			ret = -ENFILE;    // not enough, need pre-alloc
			goto out;
		}
	#ifdef FS_DEBUG
		printf("fs_open(create), fetch dentry fid = %d, inode = %d\n", (int)create_dentry->fid, (int)create_dentry->inode);
	#endif
		add_dentry_to_dirty_list(create_dentry);
		set_dentry_flag(create_dentry, D_type, FILE_DENTRY);
		create_dentry->mode = S_IFREG | 0644;
		// init the new dentry...
		char create_key[MAP_KEY_LEN];
		memset(create_key, '\0', MAP_KEY_LEN);
		sprintf(create_key, "%d", (int)p_inode);
		strcat(create_key, MAP_KEY_DELIMIT);
		strcat(create_key, cur_name);
		//create_key[charlen(create_key)] = '\0';
		uint64_t addr = (uint64_t) create_dentry;
		ret = put(&(fs_sb->tree), create_key, addr);
	#ifdef FS_DEBUG
		if (ret == 1) {
			printf("fs_open(create), put key = %s, its parent dentry inode = %d in the map!\n", create_key, (int)p_inode);
		} else {
			printf("fs_open(create), this key = %s, with parent inode = %d has already in the map\n", create_key, (int)p_inode);
		}
	#endif
		ret = SUCCESS;
		fileInfo->fh = addr;
		goto out;
	}
	ret = SUCCESS;
	fileInfo->fh = (uint64_t) dentry;
out:
	free(lkup_res);
	lkup_res = NULL;
	return ret;
}

int fs_create(const char * path, mode_t mode, struct fuse_file_info * fileInfo)
{
	int ret = 0;
	int len = strlen(path);
	int split_pos = 0;
	int i;
	for (i = len - 1; i >= 0; --i) {
		if (path[i] == '/')
			break;
	}
	split_pos = (i > 0) ? i : 1;
	char *p_path = (char *)calloc(1, split_pos + 1);
	if (i < 0)
		return -ENOENT;
	int j;
	for (j = 0; j < split_pos; ++j) {
		p_path[j] = path[j];
	}
	p_path[j] = '\0';
	char cur_name[DENTRY_NAME_SIZE];
	memset(cur_name, '\0', DENTRY_NAME_SIZE);
	if (split_pos == 1)
		memcpy(cur_name, &path[split_pos], len - split_pos);
	else
		memcpy(cur_name, &path[split_pos + 1], len - split_pos - 1);

	if (fileInfo != NULL)
		fileInfo->flags |= O_CREAT;
#ifdef FS_DEBUG
	printf("fs_create, will create path = %s, cur_name = %s\n", path, cur_name);
#endif
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
	uint32_t p_inode = lkup_res->dentry->inode;
	struct dentry *create_dentry = NULL;
	create_dentry = fetch_dentry_from_unused_list();
	if (create_dentry == NULL) {
		ret = -ENFILE;    // not enough, need pre-alloc
		goto out;
	}
#ifdef FS_DEBUG
	printf("fs_create, fetch dentry fid = %d, inode = %d\n", (int)create_dentry->fid, (int)create_dentry->inode);
#endif
	add_dentry_to_dirty_list(create_dentry);
	set_dentry_flag(create_dentry, D_type, FILE_DENTRY);
	create_dentry->mode = S_IFREG | 0644;
	// init the new dentry...
	char create_key[MAP_KEY_LEN];
	memset(create_key, '\0', MAP_KEY_LEN);
	sprintf(create_key, "%d", (int)p_inode);
	strcat(create_key, MAP_KEY_DELIMIT);
	strcat(create_key, cur_name);
	//create_key[charlen(create_key)] = '\0';
	uint64_t addr = (uint64_t) create_dentry;
	ret = put(&(fs_sb->tree), create_key, addr);
#ifdef FS_DEBUG
	if (ret == 1) {
		printf("fs_create, put key = %s, its parent dentry inode = %d in the map!\n", create_key, (int)p_inode);
	} else {
		printf("fs_create, this key = %s, with parent inode = %d has already in the map\n", create_key, (int)p_inode);
	}
#endif

	ret = SUCCESS;
	if (fileInfo != NULL)
		fileInfo->fh = addr;
out:
	free(lkup_res);
	lkup_res = NULL;
	return ret;
}

int fs_mkdir(const char *path, mode_t mode)
{
	int i;
	int ret = 0;
	int len = strlen(path);
	int split_pos = 0;
	for (i = len - 1; i >= 0; --i)
	{
		if (path[i] == '/')
			break;
	}
	split_pos = (i > 0) ? i : 1;
	char *p_path = (char *)calloc(1, split_pos + 1);
	if (i < 0)
			return -ENOENT;  // no parent dir

	int j;
	for (j = 0; j < split_pos; ++j) {
			p_path[j] = path[j];
	}
	p_path[j] = '\0';

	char cur_name[DENTRY_NAME_SIZE];
	memset(cur_name, '\0', DENTRY_NAME_SIZE);
	if (split_pos == 1)
		memcpy(cur_name, &path[split_pos], len - split_pos);
	else
		memcpy(cur_name, &path[split_pos + 1], len - split_pos - 1);
	/*
	for (i = len + 1, j = 0; i < strlen(path); i++, j++) {
		cur_name[j] = path[i];
	}
	cur_name[j] = '\0';
	*/

#ifdef FS_DEBUG
	printf("fs_mkdir, will mkdir path = %s, cur_name = %s\n", path, cur_name);
#endif
	struct dentry *dentry = NULL;
	struct lookup_res *lkup_res = NULL;
	lkup_res = (struct lookup_res *)malloc(sizeof(struct lookup_res));
	ret = path_lookup(p_path, lkup_res);
	if (ret == ERROR) {
		ret = -ENOENT;
		goto out;
	}
	dentry = lkup_res->dentry;
	if (get_dentry_flag(dentry, D_type) != DIR_DENTRY) {
		ret = -ENOTDIR;
		goto out;
	}

	uint32_t p_inode = lkup_res->dentry->inode;
	struct dentry *mkdir_dentry = NULL;
	//mkdir_dentry = fetch_dentry_from_unused_list();
	// for dir, should generate the new dentry
	mkdir_dentry = (struct dentry *) calloc(1, sizeof(struct dentry));
	mkdir_dentry->fid = 0;
	mkdir_dentry->inode = generate_unique_id();
	mkdir_dentry->flags = 0;
	set_dentry_flag(mkdir_dentry, D_type, DIR_DENTRY);
	mkdir_dentry->mode = S_IFDIR | 0755;
	mkdir_dentry->ctime = time(NULL);
	mkdir_dentry->mtime = time(NULL);
	mkdir_dentry->atime = time(NULL);
	mkdir_dentry->size = 0;
	mkdir_dentry->uid = getuid();
	mkdir_dentry->gid = getgid();
	mkdir_dentry->nlink = 0;
#ifdef FS_DEBUG
	printf("fs_mkdir, create new dir dentry id = %d, name = %s\n", (int)mkdir_dentry->inode, cur_name);
#endif
	add_dentry_to_dirty_list(mkdir_dentry);	
	// init the new dentry...
	char mkdir_key[MAP_KEY_LEN];
	memset(mkdir_key, '\0', MAP_KEY_LEN);
	sprintf(mkdir_key, "%d", (int)p_inode);
	strcat(mkdir_key, MAP_KEY_DELIMIT);
	strcat(mkdir_key, cur_name);
	uint64_t addr = (uint64_t) mkdir_dentry;
	ret = put(&(fs_sb->tree), mkdir_key, addr);
#ifdef FS_DEBUG
	if (ret == 1) {
		printf("fs_mkdir, put key = %s, its parent dentry inode = %d in the map!\n", mkdir_key, (int)p_inode);
	} else {
		printf("fs_mkdir, this key = %s, with parent inode = %d has already in the map\n", mkdir_key, (int)p_inode);
	}
#endif

	ret = SUCCESS;
out:
	free(lkup_res);
	lkup_res = NULL;
	return ret;	
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
	#ifdef FS_DEBUG
		printf("fs_opendir, open path = %s, inode = %ld\n", path, fileInfo->fh);
	#endif
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
	memset(matched_name, '\0', DENTRY_NAME_SIZE);
	memset(pre_key, '\0', MAP_PRE_KEY_LEN);
	sprintf(pre_key, "%d", (int)p_dentry->inode);
	strcat(pre_key, MAP_KEY_DELIMIT);
	int len = charlen(pre_key);
	//pre_key[len] = '\0';

#ifdef FS_DEBUG
	printf("fs_readdir, readdir path = %s, pre_key = %s\n", path, pre_key);
#endif

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
#ifdef FS_DEBUG
	printf("fs_getattr, getattr path = %s, its inode = %d\n", path, (int)dentry->inode);
#endif
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
	return -ENOSYS;
}

int fs_rename(const char *path, const char *newpath)
{
	return -ENOSYS;
}

int fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo)
{
	// a simple write implement
	int ret = 0;
	int fd = 0;
	uint64_t addr = fileInfo->fh;
	struct dentry *dentry = NULL;
	dentry = (struct dentry *) addr;

	char part[8];
	memset(part, '\0', 8);
	sprintf(part, "%d", (int)dentry->fid);
	char read_path[PATH_LEN];
	memset(read_path, '\0', PATH_LEN);
	strcpy(read_path, fs_sb->alloc_path);
	strcat(read_path, "/");
	strcat(read_path, ALLOCATED_PATH);
	strcat(read_path, "/");
	strcat(read_path, part);
	//read_path[charlen(read_path)] = '\0';

	fd = open(read_path, fileInfo->flags);
	if (unlikely(fd < 0)) {
		return -EBADF;
	}
	ret = pread(fd, buf, size, offset);
#ifdef FS_DEBUG
	printf("fs_read, read %d data from fid = %d in real path = %s\n", ret, (int)dentry->fid, read_path);
#endif
	offset += size;
	return ret;
}

int fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo)
{
	// a simple write implement
	int ret = 0;
	int fd = 0;
	uint64_t addr = fileInfo->fh;
	struct dentry *dentry = NULL;
	dentry = (struct dentry *) addr;

	char part[8];
	memset(part, '\0', 8);
	sprintf(part, "%d", (int)dentry->fid);
	char write_path[PATH_LEN];
	memset(write_path, '\0', PATH_LEN);
	strcpy(write_path, fs_sb->alloc_path);
	strcat(write_path, "/");
	strcat(write_path, ALLOCATED_PATH);
	strcat(write_path, "/");
	strcat(write_path, part);
	//write_path[charlen(write_path)] = '\0';

	fd = open(write_path, fileInfo->flags);
	if (unlikely(fd < 0)) {
		return -EBADF;
	}
	ret = pwrite(fd, buf, size, offset);
#ifdef FS_DEBUG
	printf("fs_write, write %d data from fid = %d in real path = %s\n", ret, (int)dentry->fid, write_path);
#endif
	dentry->size += size;
	offset += size;
	return ret;
}

int fs_release(const char *path, struct fuse_file_info *fileInfo)
{
	return -ENOSYS;
}

int fs_releasedir(const char * path, struct fuse_file_info * info)
{
	info->fh = -1;
	return SUCCESS;
}

int fs_utimens(const char * path, const struct timespec tv[2])
{
	int ret = 0;
	struct dentry *dentry = NULL;
	struct lookup_res *lkup_res = NULL;
	lkup_res = (struct lookup_res *)malloc(sizeof(struct lookup_res));
	ret = path_lookup(path, lkup_res);
	if (unlikely(ret == ERROR)) {
		ret = -ENOENT;
		goto out;
	}
	dentry = lkup_res->dentry;
	dentry->atime = tv[0].tv_sec;
	dentry->mtime = tv[1].tv_sec;
#ifdef FS_DEBUG
	printf("fs_utimens, update time dentry inode = %d\n", (int)dentry->inode);
#endif
out:
	free(lkup_res);
	lkup_res = NULL;
	return ret;
}

int fs_truncate(const char * path, off_t length)
{
	return -ENOSYS;
}

int fs_unlink(const char * path)
{
	return -ENOSYS;
}

int fs_chmod(const char * path, mode_t mode)
{
	return -ENOSYS;
}

int fs_chown(const char * path, uid_t owner, gid_t group)
{
	return -ENOSYS;
}

int fs_access(const char * path, int amode)
{
	return -ENOSYS;
}

int fs_symlink(const char * oldpath, const char * newpath)
{
	return -ENOSYS;
}

int fs_readlink(const char * path, char * buf, size_t size)
{
	return -ENOSYS;
}

int fs_destroy()
{

}
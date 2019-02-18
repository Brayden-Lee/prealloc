
#ifndef _MAP_H
#define _MAP_H

#include "rbtree.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>

struct map {
    struct rb_node node;
    char *key;
	uint64_t val;    // val store the addr of dentry
};

typedef struct map map_t;
typedef struct rb_root root_t;
typedef struct rb_node rb_node_t;

map_t *get(root_t *root, char *str);
int put(root_t *root, char* key, uint64_t val);
void del(root_t *root, map_t *data);

map_t *map_first(root_t *tree);
map_t *map_next(rb_node_t *node);
void map_free(map_t *node);

#endif  //_MAP_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */

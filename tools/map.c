#include "map.h"

map_t *get(root_t *root, char *str) {
   rb_node_t *node = root->rb_node; 
   while (node) {
        map_t *data = container_of(node, map_t, node);

        //compare between the key with the keys in map
        int cmp = strcmp(str, data->key);
        if (cmp < 0) {
            node = node->rb_left;
        }else if (cmp > 0) {
            node = node->rb_right;
        }else {
            return data;
        }
   }
   return NULL;
}

int put(root_t *root, char* key, uint64_t val) {
    map_t *data = (map_t*)malloc(sizeof(map_t));
	int key_len = strlen(key);
	data->key = (char *)malloc(key_len + 1);
    //data->key = (char*)malloc(sizeof(key));
    strcpy(data->key, key);
	data->key[key_len] = '\0';
	data->val = val;
    
    rb_node_t **new_node = &(root->rb_node), *parent = NULL;
    while (*new_node) {
        map_t *this_node = container_of(*new_node, map_t, node);
        int result = strcmp(key, this_node->key);
        parent = *new_node;

        if (result < 0) {
            new_node = &((*new_node)->rb_left);
        }else if (result > 0) {
            new_node = &((*new_node)->rb_right);
        }else {
            return 0;
        }
    }

    rb_link_node(&data->node, parent, new_node);
    rb_insert_color(&data->node, root);

    return 1;
}

void del(root_t *root, map_t *data) {
	rb_erase(&(data->node), root);
	map_free(data);
}

map_t *map_first(root_t *tree) {
    rb_node_t *node = rb_first(tree);
    return (rb_entry(node, map_t, node));
}

map_t *map_next(rb_node_t *node) {
    rb_node_t *next =  rb_next(node);
    return rb_entry(next, map_t, node);
}

void map_free(map_t *node){
    if (node != NULL) {
        if (node->key != NULL) {
            free(node->key);
            node->key = NULL;
    }
        free(node);
        node = NULL;
    }
}


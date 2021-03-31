#pragma once
#include <stdint.h>
#include "../btree/btree.h"

enum sh_heap_status { EMPTY_MIN_HEAP = 4, GOT_MIN_HEAP = 5, HEAP_SIZE = 32 };

struct sc_full_kv {
	struct kv_format *kv;
	uint8_t deleted;
};

struct sh_heap_node {
	union {
		struct sc_full_kv key_value;
		struct kv_prefix kv_prefix;
	};
	uint8_t level_id;
	uint8_t active_tree;
	uint8_t duplicate;
	enum KV_type type;
};

struct sh_min_heap {
	struct sh_heap_node elem[HEAP_SIZE];
	int size;
	// int active_tree;
};

struct sh_max_heap {
	struct sh_heap_node elem[HEAP_SIZE];
	int size;
	int active_tree;
};

void sh_init_heap(struct sh_min_heap *heap, int active_tree);
void sh_insert_heap_node(struct sh_min_heap *hp, struct sh_heap_node *nd);
enum sh_heap_status sh_remove_min(struct sh_min_heap *hp, struct sh_heap_node *heap_node);

void sh_init_max_heap(struct sh_max_heap *heap, int active_tree);
void sh_insert_max_heap_node(struct sh_max_heap *hp, struct sh_heap_node *nd);
enum sh_heap_status sh_remove_max(struct sh_max_heap *hp, struct sh_heap_node *heap_node);

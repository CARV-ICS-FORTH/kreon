// Copyright [2020] [FORTH-ICS]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <semaphore.h>
#include <pthread.h>
#include <stdlib.h>
#include "conf.h"
#include "../allocator/allocator.h"

#include "stats.h"

#define SUCCESS 4
#define FAILED 5

#define KREON_OK 10

#define KEY_NOT_FOUND 11

#define MAX_COUNTER_VERSIONS 4

#define MAX_HEIGHT 9

/* types used for the keys
 * KV_FORMAT: [key_len|key]
 * KV_PREFIX: [PREFIX|HASH|ADDR_TO_KV_LOG]
 */
enum KV_type { KV_FORMAT = 19, KV_PREFIX = 20 };
#define SYSTEM_ID 0
#define KV_LOG_ID 5

extern unsigned long long ins_prefix_hit_l0;
extern unsigned long long ins_prefix_hit_l1;
extern unsigned long long ins_prefix_miss_l0;
extern unsigned long long ins_prefix_miss_l1;

extern int32_t leaf_order;
extern int32_t index_order;

struct lookup_reply {
	void *addr;
	uint8_t tombstone;
};

typedef enum {
	leafNode = 590675399,
	internalNode = 790393380,
	rootNode = 742729384,
	/*special case for a newly created tree*/
	leafRootNode = 748939994,
	keyBlockHeader = 99998888,
	paddedSpace = 2222222
} nodeType_t;

enum db_status { DB_START_COMPACTION_DAEMON, DB_OPEN, DB_TERMINATE_COMPACTION_DAEMON, DB_IS_CLOSING };

/*descriptor describing a spill operation and its current status*/

typedef enum {
	NO_SPILLING = 0,
	SPILLING_IN_PROGRESS = 1,
} level_0_tree_status;

enum db_initializers { CREATE_DB = 4, DONOT_CREATE_DB = 5 };

/*
 * header of segment is 4K. L0 and KV log segments are chained in a linked list
 * with next and prev
 * pointers. garbage_bytes contains info about the unused bytes in the segment
 * due to deletes/updates.
 */
typedef struct segment_header {
	void *next_segment;
	void *prev_segment;
	uint64_t segment_id;
	uint64_t garbage_bytes[2 * MAX_COUNTER_VERSIONS];
	char pad[4008];
} segment_header;

/*Note IN stands for Internal Node*/
typedef struct IN_log_header {
	nodeType_t type;
	void *next;
} IN_log_header;

/*leaf or internal node metadata, place always in the first 4KB data block*/
typedef struct node_header {
	nodeType_t type; /*internal or leaf node*/
	/*0 are leaves, 1 are Bottom Internal nodes, and then we have
  INs and root*/
	int32_t height;
	uint64_t epoch; /*epoch of the node. It will be used for knowing when to
               perform copy on write*/
	uint64_t fragmentation;
	/*data log info, KV log for leaves private for index*/
	IN_log_header *first_IN_log_header;
	IN_log_header *last_IN_log_header;
	uint64_t key_log_size;
	uint64_t numberOfEntriesInNode;
	char pad[8];

} __attribute__((packed)) node_header;

typedef struct index_entry {
	uint64_t left[1];
	uint64_t pivot;
	uint64_t right[0];
} __attribute__((packed)) index_entry;

#define INDEX_NODE_REMAIN (INDEX_NODE_SIZE - sizeof(struct node_header))
#define LEAF_NODE_REMAIN (LEAF_NODE_SIZE - sizeof(struct node_header))

#define IN_LENGTH ((INDEX_NODE_REMAIN - sizeof(uint64_t)) / sizeof(struct index_entry) - 1)

#define LN_ITEM_SIZE (sizeof(uint64_t) + (PREFIX_SIZE * sizeof(char)))
#define LN_LENGTH (LEAF_NODE_REMAIN / LN_ITEM_SIZE)

/* this is the same as root_node */
typedef struct index_node {
	node_header header;
	index_entry p[IN_LENGTH];
	uint64_t __last_pointer; /* XXX do not use it directly! */
	char __pad[INDEX_NODE_SIZE - sizeof(struct node_header) - sizeof(uint64_t) -
		   (IN_LENGTH * sizeof(struct index_entry))];
} __attribute__((packed)) index_node;

struct kv_format {
	uint32_t key_size;
	char key_buf[];
};

struct value_format {
	uint32_t value_size;
	char value[];
};

struct kv_prefix {
	char prefix[PREFIX_SIZE];
	uint64_t device_offt : 63;
	uint64_t tombstone : 1;
};

struct leaf_kv_pointer {
	uint64_t device_offt : 63;
	uint64_t tombstone : 1;
};

/* this is the same as leaf_root_node */
typedef struct leaf_node {
	struct node_header header;
	struct leaf_kv_pointer kv_entry[LN_LENGTH];
	char prefix[LN_LENGTH][PREFIX_SIZE];
	char __pad[LEAF_NODE_SIZE - sizeof(struct node_header) - (LN_LENGTH * LN_ITEM_SIZE)];
} __attribute__((packed)) leaf_node;

/** contains info about the part of the log which has been commited but has not
 *  been applied in the index. In particular, recovery process now will be
 *  1. Read superblock, db_descriptor
 *  2. Is commit_log equal to the snapshot log?
 *      2.1 if not
 *              mark commit log segments as reserved in the allocator bitmap
 *              apply commit log changes in the index
 *              snapshot()
 *      else
 *              recover as usual
 * We now have two functions for persistence
 *      1. snapshot() persists the allocator, index, KV log, and db's of a
 *volume--> heavy operation called in minutes granularity
 *      2. commit_log() persists KV log, assuring that data in the KV-log after
 *      this operation are recoverable
 **/

#if 0
typedef struct commit_log_info {
	segment_header *first_kv_log;
	segment_header *last_kv_log;
	uint64_t kv_log_size;
	char pad[4072];
} commit_log_info;
#endif

/**
 * db_descriptor is a soft state descriptor per open database. superindex
 * structure
 * keeps a serialized from of the vital information needed to restore each
 * db_descriptor
 **/

typedef struct lock_table {
	pthread_rwlock_t rx_lock;
	char pad[8];
} lock_table;

typedef struct kv_location {
	void *kv_addr;
	uint64_t log_offset;
	uint32_t rdma_key;
} kv_location;

typedef struct level_descriptor {
#if ENABLE_BLOOM_FILTERS
	struct bloom bloom_filter[NUM_TREES_PER_LEVEL];
#endif
	lock_table guard_of_level;
	pthread_t compaction_thread[NUM_TREES_PER_LEVEL];
	lock_table *level_lock_table[MAX_HEIGHT];
	node_header *root_r[NUM_TREES_PER_LEVEL];
	node_header *root_w[NUM_TREES_PER_LEVEL];
	pthread_t spiller[NUM_TREES_PER_LEVEL];
	pthread_mutex_t spill_trigger;
	pthread_mutex_t level_allocation_lock;
	segment_header *first_segment[NUM_TREES_PER_LEVEL];
	segment_header *last_segment[NUM_TREES_PER_LEVEL];
	uint64_t offset[NUM_TREES_PER_LEVEL];
	// Since we perform always KV separation we express it
	// in number of keys
	uint64_t level_size[NUM_TREES_PER_LEVEL];
	uint64_t max_level_size;
	int64_t active_writers;
	/*spilling or not?*/
	char tree_status[NUM_TREES_PER_LEVEL];
	uint8_t active_tree;
	uint8_t level_id;
} level_descriptor;

struct bt_compaction_callback_args {
	sem_t sem;
	struct db_descriptor *db_desc;
	int src_level;
	int src_tree;
	int dst_level;
	int dst_local_tree;
	int dst_remote_tree;
};

//functions pointers for sending the index to replicas for Tebis
typedef int (*init_index_transfer)(uint64_t db_id, uint8_t level_id);
typedef int (*destroy_local_rdma_buffer)(uint64_t db_id, uint8_t level_id);
typedef int (*send_index_segment_to_replicas)(uint64_t db_id, uint64_t dev_offt, struct segment_header *seg,
					      uint32_t size, uint8_t level_id, struct node_header *root);
//typedef int (*bt_compaction_callback)(struct bt_compaction_callback_args *);
typedef int (*bt_flush_replicated_logs)(void *);

typedef struct db_descriptor {
	char db_name[MAX_DB_NAME_SIZE];
	level_descriptor levels[MAX_LEVELS];
	/*for distributed version*/
	int64_t pending_replica_operations;
	pthread_mutex_t lock_log;
	// compaction daemon staff
	pthread_t compaction_daemon;
	sem_t compaction_daemon_interrupts;
	pthread_cond_t client_barrier;
	pthread_mutex_t client_barrier_lock;
	/*for distributed version*/
	init_index_transfer idx_init;
	destroy_local_rdma_buffer destroy_rdma_buf;
	send_index_segment_to_replicas send_idx;
	//bt_compaction_callback t;
	bt_flush_replicated_logs fl;

	struct segment_header *KV_log_first_segment;
	struct segment_header *KV_log_last_segment;
	uint64_t KV_log_size;
	uint64_t latest_proposal_start_segment_offset;
	uint64_t L1_index_end_log_offset;
	struct segment_header *L1_segment;
#if 0
	commit_log_info *commit_log;
#endif
	/*how many guys have a reference to this db*/
	int32_t ref_count;
	int32_t group_id;
	int32_t group_index;
	volatile char dirty;
	/*this flag is set to 1 to let the storage engine know that its insert
	 * path is a subset of a more complex path replicated path in kreonR.
	 * if set decreasing the number of active writers for a db after
	 * SUCCESSFULL insert operation takes place outside the library. This
	 * operation takes place at the replication path after all replicas
	 * acknowledge that they have received the mutation
	 */
	char is_in_replicated_mode;
	uint8_t block_on_L0;
	enum db_status stat;
} __attribute__((packed)) __attribute__((aligned)) db_descriptor;

typedef struct db_handle {
	volume_descriptor *volume_desc;
	db_descriptor *db_desc;
} db_handle;

void set_init_index_transfer(struct db_descriptor *db_desc, init_index_transfer idx_init);
void set_destroy_local_rdma_buffer(struct db_descriptor *db_desc, destroy_local_rdma_buffer destroy_rdma_buf);
void set_send_index_segment_to_replicas(struct db_descriptor *db_desc, send_index_segment_to_replicas send_idx);
//void bt_set_compaction_callback(struct db_descriptor *db_desc, bt_compaction_callback t);
void bt_set_flush_replicated_logs_callback(struct db_descriptor *db_desc, bt_flush_replicated_logs fl);
void bt_set_inform_engine_for_pending_op_callback(struct db_descriptor *db_desc, bt_flush_replicated_logs fl);

typedef struct recovery_request {
	volume_descriptor *volume_desc;
	db_descriptor *db_desc;
	uint64_t recovery_start_log_offset;
} recovery_request;
void recovery_worker(void *);

void snapshot(volume_descriptor *volume_desc);
#if 0
void commit_db_log(db_descriptor *db_desc, commit_log_info *info);
void commit_db_logs_per_volume(volume_descriptor *volume_desc);
#endif

typedef struct rotate_data {
	node_header *left;
	node_header *right;
	void *pivot;
	int pos_left;
	int pos_right;
} rotate_data;

/*client API*/
/*management operations*/
db_handle *db_open(char *volumeName, uint64_t start, uint64_t size, char *db_name, char CREATE_FLAG);
char db_close(db_handle *handle);

void *compaction_daemon(void *args);

typedef struct bt_mutate_req {
	db_handle *handle;

	/*offset in log where the kv was written*/
	uint64_t log_offset;
	/*info for cases of segment_full_event*/
	uint64_t log_segment_addr;
	uint64_t log_offset_full_event;
	uint64_t segment_id;
	uint64_t end_of_log;
	uint32_t log_padding;
	uint32_t kv_size;
	uint8_t level_id;
	// uint32_t active_tree;
	/*only for inserts >= level_1*/
	uint8_t tree_id;
	char key_format;
	uint8_t append_to_log : 1;
	uint8_t gc_request : 1;
	uint8_t recovery_request : 1;
	/*needed for distributed version of Kreon*/
	uint8_t segment_full_event : 1;
	uint8_t special_split : 1;
	uint8_t is_tombstone : 1;
} bt_mutate_req;

typedef struct bt_insert_req {
	bt_mutate_req metadata;
	void *key_value_buf;
} bt_insert_req;

typedef struct ancestors {
	rotate_data neighbors[MAX_HEIGHT];
	node_header *parent[MAX_HEIGHT];
	int8_t node_has_key[MAX_HEIGHT];
	int size;
} ancestors;

//typedef struct delete_request {
//	bt_mutate_req metadata;
//	ancestors *ancs; /* This field is redundant and should be removed */
//	index_node *parent;
//	leaf_node *self;
//	uint64_t offset; /*offset in my parent*/
//	void *key_buf;
//} delete_request;

/* In case more operations are tracked in the log in the future such as
  transactions
  you will need to change the request_type enumerator and the log_operation
  struct.
  In the request_type you will add the name of the operation i.e. transactionOp
  and
  in the log_operation you will add a pointer in the union with the new
  operation i.e. transaction_request.
*/
typedef enum { insertOp, deleteOp, unknownOp } request_type;

typedef struct log_operation {
	bt_mutate_req *metadata;
	request_type optype_tolog;
	//union {
	bt_insert_req *ins_req;
	//delete_request *del_req;
	//};
} log_operation;

typedef struct bt_split_result {
	union {
		node_header *left_child;
		index_node *left_ichild;
		leaf_node *left_lchild;
	};

	union {
		node_header *right_child;
		index_node *right_ichild;
		leaf_node *right_lchild;
	};

	void *middle_key_buf;
	uint8_t stat;
} bt_split_result;

typedef struct metadata_tologop {
	uint32_t key_len;
	uint32_t value_len;
	uint32_t kv_size;
} metadata_tologop;

typedef struct split_data {
	node_header *father;
	node_header *son;
} split_data;

void bt_set_db_in_replicated_mode(db_handle *handle);
void bt_decrease_level0_writers(db_handle *handle);

uint8_t insert_key_value(db_handle *handle, void *key, void *value, uint32_t key_size, uint32_t value_size);
uint8_t _insert_key_value(bt_insert_req *ins_req);
void *append_key_value_to_log(log_operation *req);

uint8_t _insert_index_entry(db_handle *db, kv_location *location, int INSERT_FLAGS);
char *node_type(nodeType_t type);
void *find_key(db_handle *handle, void *key, uint32_t key_size);
void *__find_key(db_handle *handle, void *key);
int8_t delete_key(db_handle *handle, void *key, uint32_t size);

int64_t bt_key_cmp(void *key1, void *key2, char key1_format, char key2_format);
int prefix_compare(char *l, char *r, size_t unused);

/*functions used from other parts except btree/btree.c*/

void *__findKey(db_handle *handle, void *key_buf, char dirty); // dirty 0
void *_index_node_binary_search(index_node *node, void *key_buf, char query_key_format);

// void free_logical_node(allocator_descriptor *allocator_desc, node_header
// *node_index);

node_header *findLeafNode(node_header *root, void *key_buf);
void init_leaf_node(leaf_node *node);
void print_node(node_header *node);
void print_key(void *);

lock_table *_find_position(lock_table **table, node_header *node);

#define MIN(x, y) ((x > y) ? (y) : (x))
#define KEY_SIZE(x) (*(uint32_t *)x)
#define KV_MAX_SIZE (4096 + 8)
#define ABSOLUTE_ADDRESS(X) (X - MAPPED)
#define REAL_ADDRESS(X) (MAPPED + X)
#define VALUE_SIZE_OFFSET(K) (sizeof(uint32_t) + K)
#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0) YAML : 47 : 29 : error : invalid boolean

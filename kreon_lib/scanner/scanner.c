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
#include <assert.h>
#include <stdlib.h>
#include <signal.h>
#include "stack.h"
#include <log.h>
#include "scanner.h"
#include "../btree/btree.h"
#include "../btree/conf.h"

extern int32_t index_order;
extern int32_t leaf_order;

static int sc_init_level_scanner(level_scanner *level_sc, enum scanner_type, void *start_key, char seek_mode);
static int32_t sc_seek_scanner(level_scanner *level_sc, void *start_key_buf, SEEK_SCANNER_MODE mode);

level_scanner *_init_spill_buffer_scanner(db_handle *handle, node_header *node, void *start_key)
{
	level_scanner *level_sc;
	level_sc = malloc(sizeof(level_scanner));
	stack_init(&level_sc->stack);
	level_sc->db = handle;
	level_sc->root = node;

	level_sc->type = SPILL_BUFFER_SCANNER;

	/*position scanner now to the appropriate row*/
	if (sc_seek_scanner(level_sc, start_key, GREATER_OR_EQUAL) == END_OF_DATABASE) {
		log_info("empty internal buffer during spill operation, is that possible?");
		// will happen in close_spill_buffer_scanner stack_destroy(&(sc->stack));
		// free(sc);
		return NULL;
	}
	return level_sc;
}

void _close_spill_buffer_scanner(level_scanner *level_sc, node_header *root)
{
	(void)root;
	stack_destroy(&(level_sc->stack));
	free(level_sc);
}

static void init_generic_scanner(struct scannerHandle *sc, struct db_handle *handle, void *start_key, char seek_flag,
				 char dirty)
{
	struct sh_heap_node nd;
	uint8_t active_tree;
	int retval;

	if (sc == NULL) {
		log_fatal("NULL scannerHandle?");
		exit(EXIT_FAILURE);
	}

	if (!dirty && handle->db_desc->dirty)
		snapshot(handle->volume_desc);

	/*special care for level 0 due to double buffering*/
	if (dirty) {
		/*take read lock of all levels (Level-0 client writes, other for switching
    *trees
    *after compaction
    */
		// for (int i = 0; i < MAX_LEVELS; i++)
		RWLOCK_RDLOCK(&handle->db_desc->levels[0].guard_of_level.rx_lock);
	}

	for (int i = 0; i < MAX_LEVELS; i++) {
		for (int j = 0; j < NUM_TREES_PER_LEVEL; j++) {
			sc->LEVEL_SCANNERS[i][j].valid = 0;
			if (dirty)
				sc->LEVEL_SCANNERS[i][j].dirty = 1;
		}
	}

	sc->type = FULL_SCANNER;
	active_tree = handle->db_desc->levels[0].active_tree;
	sc->db = handle;
	sh_init_heap(&sc->heap, active_tree);

	for (int i = 0; i < NUM_TREES_PER_LEVEL; i++) {
		struct node_header *root;
		if (dirty) {
			if (handle->db_desc->levels[0].root_w[i] != NULL)
				root = handle->db_desc->levels[0].root_w[i];
			else
				root = handle->db_desc->levels[0].root_r[i];
		} else
			root = handle->db_desc->levels[0].root_r[i];

		if (root != NULL) {
			sc->LEVEL_SCANNERS[0][i].db = handle;
			sc->LEVEL_SCANNERS[0][i].level_id = 0;
			sc->LEVEL_SCANNERS[0][i].root = root;
			retval = sc_init_level_scanner(&(sc->LEVEL_SCANNERS[0][i]), FULL_SCANNER, start_key, seek_flag);

			if (retval == 0) {
				sc->LEVEL_SCANNERS[0][i].valid = 1;
				/*full scanners only use the heap*/
				nd.key_value = sc->LEVEL_SCANNERS[0][i].key_value;
				//log_info("Tree[%d][%d] gave us key %s", 0, i, nd.key_value.kv->key_buf);
				nd.level_id = 0;
				nd.active_tree = active_tree;
				nd.type = KV_FORMAT;
				sh_insert_heap_node(&sc->heap, &nd);
			}
		}
	}

	for (uint32_t level_id = 1; level_id < MAX_LEVELS; level_id++) {
		struct node_header *root = NULL;
		/*for persistent levels it is always the 0*/
		int tree_id = 0;
		root = handle->db_desc->levels[level_id].root_w[tree_id];
		if (!root)
			root = handle->db_desc->levels[level_id].root_r[tree_id];

		if (root != NULL) {
			sc->type = FULL_SCANNER;
			sc->LEVEL_SCANNERS[level_id][tree_id].db = handle;
			sc->LEVEL_SCANNERS[level_id][tree_id].level_id = level_id;
			sc->LEVEL_SCANNERS[level_id][tree_id].root = root;
			retval = sc_init_level_scanner(&sc->LEVEL_SCANNERS[level_id][tree_id], FULL_SCANNER, start_key,
						       seek_flag);
			if (retval == 0) {
				sc->LEVEL_SCANNERS[level_id][tree_id].valid = 1;
				nd.key_value = sc->LEVEL_SCANNERS[level_id][tree_id].key_value;
				//log_info("Tree[%d][%d] gave us key %s", level_id, 0, nd.key_value.kv->key_buf);
				nd.type = KV_FORMAT;
				// log_info("Tree[%d][%d] gave us key %s", level_id, 0, nd.KV + 4);
				nd.level_id = level_id;
				nd.active_tree = tree_id;
				sh_insert_heap_node(&sc->heap, &nd);
				sc->LEVEL_SCANNERS[level_id][tree_id].valid = 1;
			}
		}
	}

	if (getNext(sc) == END_OF_DATABASE) {
		sc->key_value.kv = NULL;
		//log_warn("Reached end of database");
		memset(&sc->key_value, 0x00, sizeof(struct sc_full_kv));
	}
	return;
}

/*no snaphsot scanner (with lock)*/
void init_dirty_scanner(struct scannerHandle *sc, struct db_handle *handle, void *start_key, char seek_flag)
{
	init_generic_scanner(sc, handle, start_key, seek_flag, 1);
	return;
}

scannerHandle *initScanner(scannerHandle *sc, db_handle *handle, void *start_key, char seek_flag)
{
	if (sc == NULL) {
		log_fatal("NULL scannerHandle");
		exit(EXIT_FAILURE);
	}
	init_generic_scanner(sc, handle, start_key, seek_flag, 0);
	return sc;
}

static int sc_init_level_scanner(level_scanner *level_sc, enum scanner_type type, void *start_key, char seek_mode)
{
	stack_init(&level_sc->stack);
	level_sc->type = type;
	if (sc_seek_scanner(level_sc, start_key, seek_mode) == END_OF_DATABASE) {
		// log_info("EMPTY DATABASE after seek for key %u:%s!", *(uint32_t
		// *)start_key,
		//	 start_key + sizeof(uint32_t));
		stack_destroy(&(level_sc->stack));
		return -1;
	}
	return 0;
}

static void read_lock_node(struct level_scanner *level_sc, struct node_header *node)
{
	if (!level_sc->dirty)
		return;
	if (level_sc->level_id > 0)
		return;

	struct lock_table *lock = _find_position(level_sc->db->db_desc->levels[0].level_lock_table, node);
	if (RWLOCK_RDLOCK(&lock->rx_lock) != 0) {
		log_fatal("ERROR locking");
		exit(EXIT_FAILURE);
	}
	return;
}

static void read_unlock_node(struct level_scanner *level_sc, struct node_header *node)
{
	if (!level_sc->dirty)
		return;
	if (level_sc->level_id > 0)
		return;

	struct lock_table *lock = _find_position(level_sc->db->db_desc->levels[0].level_lock_table, node);
	if (RWLOCK_UNLOCK(&lock->rx_lock) != 0) {
		log_fatal("ERROR locking");
		exit(EXIT_FAILURE);
	}
	return;
}

void closeScanner(scannerHandle *sc)
{
	stackElementT stack_top;
	/*special care for L0*/
	for (int i = 0; i < NUM_TREES_PER_LEVEL; i++) {
		if (sc->LEVEL_SCANNERS[0][i].valid && sc->LEVEL_SCANNERS[0][i].dirty) {
			while (1) {
				stack_top = stack_pop(&(sc->LEVEL_SCANNERS[0][i].stack));
				if (stack_top.guard)
					break;
				read_unlock_node(&sc->LEVEL_SCANNERS[0][i], stack_top.node);
			}
		}
		stack_destroy(&(sc->LEVEL_SCANNERS[0][i].stack));
	}

	for (int i = 1; i < MAX_LEVELS; i++) {
		if (sc->LEVEL_SCANNERS[i][0].valid) {
			stack_destroy(&(sc->LEVEL_SCANNERS[i][0].stack));
		}
	}
	/*finally*/
	if (sc->LEVEL_SCANNERS[0][0].dirty) {
		// for (int i = 0; i < MAX_LEVELS; i++)
		RWLOCK_UNLOCK(&sc->db->db_desc->levels[0].guard_of_level.rx_lock);
	}
	return;
}

void close_dirty_scanner(scannerHandle *sc)
{
	closeScanner(sc);

	struct db_descriptor *db_desc = sc->db->db_desc;
	for (int i = 0; i < MAX_LEVELS; i++)
		RWLOCK_UNLOCK(&db_desc->levels[i].guard_of_level.rx_lock);
}

int isValid(scannerHandle *sc)
{
	if (sc->key_value.kv != NULL)
		return 1;
	return 0;
}

int32_t getKeySize(scannerHandle *sc)
{
	return sc->key_value.kv->key_size;
}

void *getKeyPtr(scannerHandle *sc)
{
	return sc->key_value.kv->key_buf;
}

int32_t getValueSize(scannerHandle *sc)
{
	int32_t *val_ptr = (int32_t *)((char *)((uint64_t)sc->key_value.kv) + sizeof(struct kv_format) +
				       sc->key_value.kv->key_size);
	return *val_ptr;
}

void *getValuePtr(scannerHandle *sc)
{
	void *val_ptr = ((char *)((uint64_t)sc->key_value.kv) + sizeof(struct kv_format) + sc->key_value.kv->key_size +
			 sizeof(uint32_t));
	return val_ptr + sizeof(int32_t);
}

static int64_t sc_compare_with_current(struct level_scanner *level_sc, struct kv_format *key)
{
	switch (level_sc->type) {
	case SPILL_BUFFER_SCANNER:
		return bt_key_cmp(&level_sc->kv_prefix, key, KV_PREFIX, KV_FORMAT);
	default:
		return bt_key_cmp(level_sc->key_value.kv, key, KV_FORMAT, KV_FORMAT);
	}
}

static int32_t sc_seek_scanner(level_scanner *level_sc, void *start_key_buf, SEEK_SCANNER_MODE mode)
{
	char key_buf_prefix[PREFIX_SIZE];
	stackElementT element;
	void *full_pivot_key;
	void *addr = NULL;
	char *index_key_prefix;
	index_node *inode;
	leaf_node *lnode;
	node_header *node;
	int64_t ret;
	int32_t start_idx = 0;
	int32_t end_idx = 0;
	int32_t middle;

	/*drop all paths*/
	stack_reset(&(level_sc->stack));
	/*put guard*/
	element.guard = 1;
	element.leftmost = 0;
	element.rightmost = 0;
	element.idx = 0;
	element.node = NULL;

	stack_push(&(level_sc->stack), element);
	/* for L0 already safe we have read lock of guard lock
   * else its just a root_r of levels >= 1
   */
	node = level_sc->root;
	read_lock_node(level_sc, node);
	//assert(node->type == rootNode);

	if (node->type == leafRootNode && node->numberOfEntriesInNode == 0) {
		/*we seek in an empty tree*/
		read_unlock_node(level_sc, node);
		return END_OF_DATABASE;
	}

	while (node->type != leafNode && node->type != leafRootNode) {
		inode = (index_node *)node;
		start_idx = 0;
		end_idx = inode->header.numberOfEntriesInNode - 1;
		// middle = (start_idx + end_idx) / 2;

		while (1) {
			middle = (start_idx + end_idx) / 2;
			/*reconstruct full key*/
			addr = &(inode->p[middle].pivot);
			full_pivot_key = (void *)(MAPPED + *(uint64_t *)addr);
			ret = bt_key_cmp(full_pivot_key, start_key_buf, KV_FORMAT, KV_FORMAT);
			// log_info("pivot %u:%s app %u:%s ret %lld", *(uint32_t
			// *)(full_pivot_key), full_pivot_key + 4,
			//	 *(uint32_t *)start_key_buf, start_key_buf + 4, ret);

			if (ret == 0) {
				break;
			} else if (ret > 0) {
				end_idx = middle - 1;
				if (start_idx > end_idx)
					break;
			} else {
				start_idx = middle + 1;
				if (start_idx > end_idx)
					break;
			}
		}

		element.node = node;
		element.guard = 0;
		int num_entries = node->numberOfEntriesInNode;
		/*the path we need to follow*/
		if (ret <= 0)
			node = (node_header *)(MAPPED + inode->p[middle].right[0]);
		else
			node = (node_header *)(MAPPED + inode->p[middle].left[0]);

		read_lock_node(level_sc, node);

		/*cornercases leftmost path and rightmost path*/
		if (middle == 0 && ret > 0) {
			/*first path of node*/
			// log_info("leftmost path");
			element.leftmost = 1;
			element.rightmost = 0;
			element.idx = 0;
		} else if (middle >= (int64_t)num_entries - 1 && ret <= 0) {
			/*last path of node*/
			// log_info("rightmost path middle = %d num entries = %d",middle,
			// num_entries);
			// log_info("pivot %s seek key %s",full_pivot_key+4,start_key_buf+4);
			element.leftmost = 0;
			element.rightmost = 1;
			element.idx = middle;
		} else {
			element.leftmost = 0;
			element.rightmost = 0;
			if (ret > 0)
				element.idx = --middle;
			else
				element.idx = middle;
		}
		stack_push(&(level_sc->stack), element);
	}

	/*reached leaf node, lock already there setup prefixes*/
	if (start_key_buf == NULL)
		memset(key_buf_prefix, 0, PREFIX_SIZE * sizeof(char));
	else {
		uint32_t s_key_size = *(uint32_t *)start_key_buf;
		if (s_key_size >= PREFIX_SIZE)
			memcpy(key_buf_prefix, (void *)((uint64_t)start_key_buf + sizeof(int32_t)), PREFIX_SIZE);
		else {
			s_key_size = *(uint32_t *)start_key_buf;
			memcpy(key_buf_prefix, (void *)((uint64_t)start_key_buf + sizeof(int32_t)), s_key_size);
			memset(key_buf_prefix + s_key_size, 0x00, PREFIX_SIZE - s_key_size);
		}
	}

	/*now perform binary search inside the leaf*/
	lnode = (leaf_node *)node;
	start_idx = 0;
	end_idx = lnode->header.numberOfEntriesInNode - 1;
	middle = 0;

	while (start_idx <= end_idx) {
		middle = (start_idx + end_idx) / 2;

		index_key_prefix = &lnode->prefix[middle][0];
		ret = prefix_compare(index_key_prefix, key_buf_prefix, PREFIX_SIZE);
		// log_info("index prefix %s key pref %s ret %d", index_key_prefix,
		// key_buf_prefix, ret);
		if (ret < 0) {
			start_idx = middle + 1;
			// if (start_idx > end_idx) {
			//	middle++;
			//	break;
			//}
		} else if (ret > 0) {
			end_idx = middle - 1;
			// if (start_idx > end_idx)
			//	break;
		} else {
			/*prefix is the same*/
			addr = (void *)(MAPPED + lnode->kv_entry[middle].device_offt);
			ret = bt_key_cmp(addr, start_key_buf, KV_FORMAT, KV_FORMAT);

			if (ret == 0) {
				break;
			} else if (ret < 0) {
				start_idx = middle + 1;
				if (start_idx > end_idx) {
					middle++;
					break;
				}
			} else if (ret > 0) {
				end_idx = middle - 1;
				if (start_idx > end_idx)
					break;
			}
		}
	}

	/*further checks*/
	if (middle <= 0 && lnode->header.numberOfEntriesInNode > 1) {
		element.node = node;
		element.idx = 0;
		element.leftmost = 1;
		element.rightmost = 0;
		element.guard = 0;
		// log_debug("Leftmost boom");
		stack_push(&(level_sc->stack), element);
		middle = 0;
	} else if (middle >= (int64_t)lnode->header.numberOfEntriesInNode - 1) {
		// log_info("rightmost");
		//middle = lnode->header.numberOfEntriesInNode - 1;
		element.node = node;
		element.idx = 0;
		element.leftmost = 0;
		element.rightmost = 1;
		element.guard = 0;
		stack_push(&(level_sc->stack), element);
		middle = lnode->header.numberOfEntriesInNode - 1;
	} else {
		// log_info("middle is %d", middle);
		element.node = node;
		element.idx = middle;
		element.leftmost = 0;
		element.rightmost = 0;
		element.guard = 0;
		stack_push(&(level_sc->stack), element);
	}
	switch (level_sc->type) {
	case FULL_SCANNER:
		level_sc->key_value.kv = (struct kv_format *)(MAPPED + lnode->kv_entry[middle].device_offt);
		level_sc->key_value.deleted = lnode->kv_entry[middle].tombstone;
		break;
	case SPILL_BUFFER_SCANNER:
		memcpy(level_sc->kv_prefix.prefix, &lnode->prefix[middle][0], PREFIX_SIZE);
		level_sc->kv_prefix.device_offt = lnode->kv_entry[middle].device_offt;
		level_sc->kv_prefix.tombstone = lnode->kv_entry[middle].tombstone;
		break;
	default:
		log_fatal("unknown scanner type is %d", level_sc->type);
		raise(SIGINT);
		exit(EXIT_FAILURE);
	}

	if (start_key_buf != NULL) {
		switch (mode) {
		case GREATER:
			while (sc_compare_with_current(level_sc, start_key_buf) <= 0) {
				if (_get_next_KV(level_sc) == END_OF_DATABASE)
					return END_OF_DATABASE;
			}
			break;
		case GREATER_OR_EQUAL:
			while (sc_compare_with_current(level_sc, start_key_buf) < 0) {
				if (_get_next_KV(level_sc) == END_OF_DATABASE)
					return END_OF_DATABASE;
			}
			break;
		default:
			log_fatal("unknown seek mode");
			exit(EXIT_FAILURE);
		}
	}
	return SUCCESS;
}

int32_t getNext(scannerHandle *sc)
{
	enum sh_heap_status stat;
	struct sh_heap_node nd;
	struct sh_heap_node next_nd;

	while (1) {
		stat = sh_remove_min(&sc->heap, &nd);
		if (stat != EMPTY_MIN_HEAP) {
			sc->key_value = nd.key_value;
			if (_get_next_KV(&(sc->LEVEL_SCANNERS[nd.level_id][nd.active_tree])) != END_OF_DATABASE) {
				// log_info("refilling from level_id %d\n", nd.level_id);
				next_nd.level_id = nd.level_id;
				next_nd.active_tree = nd.active_tree;
				next_nd.type = nd.type;
				next_nd.key_value = sc->LEVEL_SCANNERS[nd.level_id][nd.active_tree].key_value;
				sh_insert_heap_node(&sc->heap, &next_nd);
			}
			if (nd.duplicate == 1 || nd.key_value.deleted) {
				// assert(0);
				// log_warn("ommiting duplicate %s", (char *)nd.data + 4);
				continue;
			}
			return KREON_OK;
		} else {
			sc->key_value.kv = NULL;
			return END_OF_DATABASE;
		}
	}
}

int32_t _get_next_KV(level_scanner *sc)
{
	stackElementT stack_top;
	node_header *node;
	index_node *inode;
	leaf_node *lnode;
	uint32_t idx;
	uint32_t up = 1;

	stack_top = stack_pop(&(sc->stack)); /*get the element*/
	if (stack_top.guard) {
		switch (sc->type) {
		case FULL_SCANNER:
			sc->key_value.kv = NULL;
			break;
		case SPILL_BUFFER_SCANNER:
			memset(&sc->kv_prefix, 0x00, sizeof(struct kv_prefix));
			break;
		default:
			log_fatal("Unknown scanner mode");
			exit(EXIT_FAILURE);
		}
		return END_OF_DATABASE;
	}

	if (stack_top.node->type != leafNode && stack_top.node->type != leafRootNode) {
		log_fatal("Corrupted scanner stack, top element should be a leaf node");
		exit(EXIT_FAILURE);
	}
	node = stack_top.node;
	// log_info("stack top rightmost %d leftmost %d", stack_top.rightmost,
	// stack_top.leftmost);
	while (1) {
		if (up) {
			/*check if we can advance in the current node*/
			if (stack_top.rightmost) {
				read_unlock_node(sc, stack_top.node);

				stack_top = stack_pop(&(sc->stack));
				if (!stack_top.guard) {
					// log_debug("rightmost in stack throw and continue type %s",
					//	  node_type(stack_top.node->type));
					continue;
				} else {
					return END_OF_DATABASE;
				}
			} else if (stack_top.leftmost) {
				// log_debug("leftmost? %s", node_type(stack_top.node->type));
				stack_top.leftmost = 0;

				if (stack_top.node->type == leafNode || stack_top.node->type == leafRootNode) {
					// log_info("got a leftmost leaf advance");
					if (stack_top.node->numberOfEntriesInNode > 1) {
						idx = 1;
						stack_top.idx = 1;
						if (node->numberOfEntriesInNode == 2)
							stack_top.rightmost = 1;
						node = stack_top.node;
						stack_push(&sc->stack, stack_top);
						break;
					} else {
						stack_top = stack_pop(&(sc->stack));
						if (!stack_top.guard) {
							// log_debug("rightmost in stack throw and continue type %s",
							//	  node_type(stack_top.node->type));
							continue;
						} else
							return END_OF_DATABASE;
					}
				} else if (stack_top.node->type == internalNode || stack_top.node->type == rootNode) {
					// log_debug("Calculate and push type %s",
					// node_type(stack_top.node->type));
					/*special case applies only for the root*/
					if (stack_top.node->numberOfEntriesInNode == 1)
						stack_top.rightmost = 1;
					stack_top.idx = 0;
					stack_push(&sc->stack, stack_top);
					inode = (index_node *)stack_top.node;
					node = (node_header *)(MAPPED + inode->p[0].right[0]);
					assert(node->type == rootNode || node->type == leafRootNode ||
					       node->type == internalNode || node->type == leafNode);
					// stack_top.node = node;
					// log_debug("Calculate and push type %s",
					// node_type(stack_top.node->type));
					// stack_push(&sc->stack, stack_top);
					up = 0;
					continue;
				} else {
					log_fatal("Corrupted node");
					assert(0);
					exit(EXIT_FAILURE);
				}
			} else {
				// log_debug("Advancing, %s idx = %d entries %d",
				// node_type(stack_top.node->type),
				//	  stack_top.idx, stack_top.node->numberOfEntriesInNode);
				++stack_top.idx;
				if (stack_top.idx >= stack_top.node->numberOfEntriesInNode - 1)
					stack_top.rightmost = 1;
			}
			stack_push(&sc->stack, stack_top);

			if (stack_top.node->type == leafNode || stack_top.node->type == leafRootNode) {
				idx = stack_top.idx;
				node = stack_top.node;
				break;
			} else if (stack_top.node->type == internalNode || stack_top.node->type == rootNode) {
				inode = (index_node *)stack_top.node;
				node = (node_header *)(MAPPED + (uint64_t)inode->p[stack_top.idx].right[0]);
				up = 0;
				assert(node->type == rootNode || node->type == leafRootNode ||
				       node->type == internalNode || node->type == leafNode);
				continue;
			} else {
				log_fatal("Corrupted node");
				assert(0);
				exit(EXIT_FAILURE);
			}
		} else {
			/*push yourself, update node and continue*/

			stack_top.node = node;

			read_lock_node(sc, stack_top.node);

			// log_debug("Saved type %s", node_type(stack_top.node->type));
			stack_top.idx = 0;
			stack_top.leftmost = 1;
			stack_top.rightmost = 0;
			stack_top.guard = 0;
			stack_push(&sc->stack, stack_top);
			if (node->type == leafNode || node->type == leafRootNode) {
				// log_info("consumed first entry of leaf");
				idx = 0;
				break;
			} else if (node->type == internalNode || node->type == rootNode) {
				inode = (index_node *)node;
				node = (node_header *)(MAPPED + (uint64_t)inode->p[0].left[0]);
			} else {
				log_fatal("Reached corrupted node");
				assert(0);
			}
		}
	}
	lnode = (leaf_node *)node;
	// log_warn("Key %lu:%s idx is %d", *(uint32_t *)(MAPPED +
	// (uint64_t)lnode->pointer[idx]),
	// MAPPED + lnode->pointer[idx] + 4, idx);
	/*fill buffer and return*/
	switch (sc->type) {
	case FULL_SCANNER:
		sc->key_value.kv = (void *)MAPPED + lnode->kv_entry[idx].device_offt;
		sc->key_value.deleted = lnode->kv_entry[idx].tombstone;
		break;
	case SPILL_BUFFER_SCANNER:
		memcpy(sc->kv_prefix.prefix, lnode->prefix[idx], PREFIX_SIZE);
		sc->kv_prefix.device_offt = lnode->kv_entry[idx].device_offt;
		sc->kv_prefix.tombstone = lnode->kv_entry[idx].tombstone;
		break;
	default:
		log_fatal("unknown scanner type");
		raise(SIGINT);
		exit(EXIT_FAILURE);
		break;
	}

	return SUCCESS;
}

int32_t _get_prev_KV(level_scanner *sc)
{
	stackElementT stack_top;
	node_header *node;
	index_node *inode;
	leaf_node *lnode;
	uint32_t idx;
	uint32_t up = 1;
	stack_top = stack_pop(&(sc->stack)); /*get the element*/
	if (stack_top.guard) {
		sc->key_value.kv = NULL;
		return END_OF_DATABASE;
	}

	if (stack_top.node->type != leafNode && stack_top.node->type != leafRootNode) {
		log_fatal("Corrupted scanner stack, top element should be a leaf node");
		raise(SIGINT);
		exit(EXIT_FAILURE);
	}

	node = stack_top.node;
	//log_info("stack top rightmost %d leftmost %d", stack_top.rightmost, stack_top.leftmost);
	while (1) {
		if (up) {
			/*check if we can advance in the current node*/
			if (stack_top.leftmost) {
				read_unlock_node(sc, stack_top.node);

				stack_top = stack_pop(&(sc->stack));
				//printf("rightmost? %s", stack_top.node->type);
				//node_type(stack_top.node->type);
				if (!stack_top.guard) {
					continue;
				} else {
					return END_OF_DATABASE;
				}
			} else if (stack_top.rightmost) {
				//log_debug("rightmost? %s", node_type(stack_top.node->type));
				stack_top.rightmost = 0;
				if (stack_top.node->type == leafNode || stack_top.node->type == leafRootNode) {
					//log_info("got a rightmost leaf advance");
					if (stack_top.node->numberOfEntriesInNode > 1) {
						idx = stack_top.node->numberOfEntriesInNode - 2;
						stack_top.idx = stack_top.node->numberOfEntriesInNode - 2;
						if (node->numberOfEntriesInNode == 2)
							stack_top.leftmost = 1;
						node = stack_top.node;
						stack_push(&sc->stack, stack_top);
						break;
					} else {
						stack_top = stack_pop(&(sc->stack));
						if (!stack_top.guard) {
							continue;
						} else
							return END_OF_DATABASE;
					}
				} else if (stack_top.node->type == internalNode || stack_top.node->type == rootNode) {
					if (stack_top.node->numberOfEntriesInNode == 1)
						stack_top.leftmost = 1;
					stack_top.idx = stack_top.node->numberOfEntriesInNode - 1;
					stack_push(&sc->stack, stack_top);
					inode = (index_node *)stack_top.node;
					node = (node_header *)(MAPPED + inode->p[stack_top.idx].left[0]);
					assert(node->type == rootNode || node->type == leafRootNode ||
					       node->type == internalNode || node->type == leafNode);
					up = 0;
					continue;
				} else {
					log_fatal("Corrupted node");
					assert(0);
				}
			} else {
				--stack_top.idx;
				if (stack_top.idx <= 0)
					stack_top.leftmost = 1;
			}
			stack_push(&sc->stack, stack_top);
			if (stack_top.node->type == leafNode || stack_top.node->type == leafRootNode) {
				idx = stack_top.idx;
				node = stack_top.node;
				break;
			} else if (stack_top.node->type == internalNode || stack_top.node->type == rootNode) {
				inode = (index_node *)stack_top.node;
				node = (node_header *)(MAPPED + (uint64_t)inode->p[stack_top.idx].left[0]);
				up = 0;
				assert(node->type == rootNode || node->type == leafRootNode ||
				       node->type == internalNode || node->type == leafNode);
				continue;
			} else {
				log_fatal("Corrupted node");
				assert(0);
				exit(EXIT_FAILURE);
			}
		} else {
			stack_top.node = node;

			read_lock_node(sc, stack_top.node);

			stack_top.idx = stack_top.node->numberOfEntriesInNode - 1;
			stack_top.leftmost = 0;
			stack_top.rightmost = 1;
			stack_top.guard = 0;
			stack_push(&sc->stack, stack_top);
			if (node->type == leafNode || node->type == leafRootNode) {
				idx = stack_top.node->numberOfEntriesInNode - 1;
				break;
			} else if (node->type == internalNode || node->type == rootNode) {
				inode = (index_node *)node;
				node = (node_header *)(MAPPED + (uint64_t)inode->p[stack_top.idx].right[0]);
			} else {
				log_fatal("Reached corrupted node");
				assert(0);
			}
		}
	}

	lnode = (leaf_node *)node;

	switch (sc->type) {
	case FULL_SCANNER:
		sc->key_value.kv = (void *)MAPPED + lnode->kv_entry[idx].device_offt;
		sc->key_value.deleted = lnode->kv_entry[idx].tombstone;
		break;
	case SPILL_BUFFER_SCANNER:
		memcpy(sc->kv_prefix.prefix, lnode->prefix[idx], PREFIX_SIZE);
		sc->kv_prefix.device_offt = lnode->kv_entry[idx].device_offt;
		sc->kv_prefix.tombstone = lnode->kv_entry[idx].tombstone;
		break;
	default:
		log_fatal("unknown scanner type");
		raise(SIGINT);
		exit(EXIT_FAILURE);
		break;
	}

	return SUCCESS;
}

static int32_t SeekLast(level_scanner *level_sc, void *start_key_buf)
{
	char key_buf_prefix[PREFIX_SIZE];
	stackElementT element;
	//void *full_pivot_key;
	void *addr = NULL;
	char *index_key_prefix;
	index_node *inode;
	leaf_node *lnode;
	node_header *node;
	int64_t ret;
	int32_t start_idx = 0;
	int32_t end_idx = 0;
	int32_t middle;
	//char level_key_format;
	stack_reset(&(level_sc->stack));
	element.guard = 1;
	element.leftmost = 0;
	element.rightmost = 0;
	element.idx = 0;
	element.node = NULL;
	stack_push(&(level_sc->stack), element);
	node = level_sc->root;

	read_lock_node(level_sc, node);

	if (node->type == leafRootNode && node->numberOfEntriesInNode == 0) {
		read_unlock_node(level_sc, node);
		return END_OF_DATABASE;
	}

	while (node->type != leafNode && node->type != leafRootNode) {
		inode = (index_node *)node;
		int pivot = inode->header.numberOfEntriesInNode - 1;
		element.leftmost = 0;
		element.rightmost = 1;
		element.node = node;
		element.guard = 0;
		//element.idx = inode->header.numberOfEntriesInNode - 1;
		node = (node_header *)(MAPPED + inode->p[pivot].right[0]);
		stack_push(&(level_sc->stack), element);
	}

	if (start_key_buf == NULL)
		memset(key_buf_prefix, 0, PREFIX_SIZE * sizeof(char));
	else {
		uint32_t s_key_size = *(uint32_t *)start_key_buf;
		if (s_key_size >= PREFIX_SIZE)
			memcpy(key_buf_prefix, (void *)((uint64_t)start_key_buf + sizeof(int32_t)), PREFIX_SIZE);
		else {
			s_key_size = *(uint32_t *)start_key_buf;
			memcpy(key_buf_prefix, (void *)((uint64_t)start_key_buf + sizeof(int32_t)), s_key_size);
			memset(key_buf_prefix + s_key_size, 0x00, PREFIX_SIZE - s_key_size);
		}
	}

	lnode = (leaf_node *)node;
	start_idx = 0;
	end_idx = lnode->header.numberOfEntriesInNode - 1;
	middle = 0;

	while (start_idx <= end_idx) {
		middle = (start_idx + end_idx) / 2;
		index_key_prefix = &lnode->prefix[middle][0];
		ret = prefix_compare(index_key_prefix, key_buf_prefix, PREFIX_SIZE);
		if (ret < 0) {
			start_idx = middle + 1;
		} else if (ret > 0) {
			end_idx = middle - 1;
		} else {
			addr = (void *)(MAPPED + lnode->kv_entry[middle].device_offt);
			ret = bt_key_cmp(addr, start_key_buf, KV_FORMAT, KV_FORMAT);

			if (ret == 0) {
				break;
			} else if (ret < 0) {
				start_idx = middle + 1;
				if (start_idx > end_idx) {
					middle++;
					break;
				}
			} else if (ret > 0) {
				end_idx = middle - 1;
				if (start_idx > end_idx)
					break;
			}
		}
	}

	if (middle <= 0 && lnode->header.numberOfEntriesInNode > 1) {
		element.node = node;
		element.idx = lnode->header.numberOfEntriesInNode - 1;
		element.leftmost = 0;
		element.rightmost = 1;
		element.guard = 0;
		stack_push(&(level_sc->stack), element);
		middle = 0;
	} else if (middle >= (int64_t)lnode->header.numberOfEntriesInNode - 1) {
		middle = lnode->header.numberOfEntriesInNode - 1;
		element.node = node;
		element.idx = 0;
		element.leftmost = 0;
		element.rightmost = 1;
		element.guard = 0;
		stack_push(&(level_sc->stack), element);
		middle = lnode->header.numberOfEntriesInNode - 1;
	} else {
		element.node = node;
		element.idx = middle;
		element.leftmost = 1;
		element.rightmost = 0;
		element.guard = 0;
		stack_push(&(level_sc->stack), element);
	}

	middle = lnode->header.numberOfEntriesInNode - 1;
	level_sc->key_value.kv = (struct kv_format *)(MAPPED + lnode->kv_entry[middle].device_offt);
	level_sc->key_value.deleted = lnode->kv_entry[middle].tombstone;

	//level_key_format = KV_FORMAT;
	return KREON_OK;
}

static int FindLast(level_scanner *level_sc, enum scanner_type type, void *start_key)
{
	stack_init(&level_sc->stack);
	level_sc->type = type;
	if (SeekLast(level_sc, start_key) == END_OF_DATABASE) {
		stack_destroy(&(level_sc->stack));
		return -1;
	}
	return 0;
}

void seek_to_last(db_handle *handle, struct Kreoniterator *it)
{
	it->sc = (scannerHandle *)malloc(sizeof(scannerHandle));
	struct sh_heap_node nd;
	uint8_t active_tree;
	int retval;

	if (it->sc == NULL) {
		log_fatal("NULL scannerHandle?");
		exit(EXIT_FAILURE);
	}

	RWLOCK_RDLOCK(&handle->db_desc->levels[0].guard_of_level.rx_lock);

	for (int i = 0; i < MAX_LEVELS; i++) {
		for (int j = 0; j < NUM_TREES_PER_LEVEL; j++) {
			it->sc->LEVEL_SCANNERS[i][j].valid = 0;
		}
	}

	it->sc->type = FULL_SCANNER;
	active_tree = handle->db_desc->levels[0].active_tree;
	it->sc->db = handle;
	sh_init_max_heap(&it->sc->max_heap, active_tree);
	for (int i = 0; i < NUM_TREES_PER_LEVEL; i++) {
		struct node_header *root;

		if (handle->db_desc->levels[0].root_w[i] != NULL)
			root = handle->db_desc->levels[0].root_w[i];
		else
			root = handle->db_desc->levels[0].root_r[i];

		if (root != NULL) {
			it->sc->LEVEL_SCANNERS[0][i].db = handle;
			it->sc->LEVEL_SCANNERS[0][i].level_id = 0;
			it->sc->LEVEL_SCANNERS[0][i].root = root;
			retval = FindLast(&(it->sc->LEVEL_SCANNERS[0][i]), FULL_SCANNER, NULL);

			if (retval == 0) {
				it->sc->LEVEL_SCANNERS[0][i].valid = 1;
				nd.key_value = it->sc->LEVEL_SCANNERS[0][i].key_value;
				nd.level_id = 0;
				nd.type = KV_FORMAT;
				nd.active_tree = active_tree;
				sh_insert_max_heap_node(&it->sc->max_heap, &nd);
			}
		}
	}

	for (uint32_t level_id = 1; level_id < MAX_LEVELS; level_id++) {
		struct node_header *root = NULL;
		int tree_id = 0;
		root = handle->db_desc->levels[level_id].root_w[tree_id];
		if (!root)
			root = handle->db_desc->levels[level_id].root_r[tree_id];
		if (root != NULL) {
			it->sc->LEVEL_SCANNERS[level_id][tree_id].db = handle;
			it->sc->LEVEL_SCANNERS[level_id][tree_id].level_id = level_id;
			it->sc->LEVEL_SCANNERS[level_id][tree_id].root = root;
			retval = FindLast(&it->sc->LEVEL_SCANNERS[level_id][tree_id], FULL_SCANNER, NULL);
			if (retval == 0) {
				it->sc->LEVEL_SCANNERS[level_id][tree_id].valid = 1;
				nd.key_value = it->sc->LEVEL_SCANNERS[level_id][tree_id].key_value;
				nd.type = KV_FORMAT;
				nd.level_id = level_id;
				nd.active_tree = tree_id;
				//sh_insert_heap_node(&it->sc->heap,&nd);
				sh_insert_max_heap_node(&it->sc->max_heap, &nd);
				it->sc->LEVEL_SCANNERS[level_id][tree_id].valid = 1;
			}
		}
	}
}

void seek_to_first(db_handle *handle, struct Kreoniterator *it)
{
	char smallest_char = '0';
	it->sc = (scannerHandle *)malloc(sizeof(scannerHandle));

	init_dirty_scanner(it->sc, handle, &smallest_char, GREATER_OR_EQUAL);
}

int get_next(struct Kreoniterator *it)
{
	if (it->sc == NULL) {
		log_fatal("Corrupted node");
		assert(0);
		exit(EXIT_FAILURE);
	}

	return getNext(it->sc);
}

int get_prev(struct Kreoniterator *it)
{
	enum sh_heap_status stat;
	struct sh_heap_node nd;
	struct sh_heap_node next_nd;
	while (1) {
		stat = sh_remove_max(&it->sc->max_heap, &nd);
		if (stat != EMPTY_MIN_HEAP) {
			it->sc->key_value = nd.key_value;
			if (_get_prev_KV(&(it->sc->LEVEL_SCANNERS[nd.level_id][nd.active_tree])) != END_OF_DATABASE) {
				next_nd.level_id = nd.level_id;
				next_nd.active_tree = nd.active_tree;
				next_nd.type = nd.type;
				next_nd.key_value = it->sc->LEVEL_SCANNERS[nd.level_id][nd.active_tree].key_value;
				sh_insert_max_heap_node(&it->sc->max_heap, &next_nd);
			}
			if (nd.duplicate == 1 || nd.key_value.deleted) {
				continue;
			}
			return KREON_OK;
		} else {
			it->sc->key_value.kv = NULL;
			return END_OF_DATABASE;
		}
	}
}

int Seek(db_handle *handle, void *Keyname, struct Kreoniterator *it)
{
	it->sc = (scannerHandle *)malloc(sizeof(scannerHandle));

	init_dirty_scanner(it->sc, handle, Keyname, GREATER_OR_EQUAL);
	return 1;
}

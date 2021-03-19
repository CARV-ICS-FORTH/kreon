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

#include "gc.h"

char *pointer_to_kv_in_log = NULL;

void push_stack(stack *marks, void *addr)
{
	marks->valid_pairs[marks->size++] = addr;
	assert(marks->size != STACK_SIZE);
}

/* TODO use the marks stack instead of reiterating
   the whole segment to find the non deleted kv pairs. */
void move_kv_pairs_to_new_segment(volume_descriptor *volume_desc, db_descriptor *db_desc, stack *marks)
{
	bt_insert_req ins_req;

	db_handle handle = { .volume_desc = volume_desc, .db_desc = db_desc };
	char *kv_address;
	int i;

	for (i = 0; i < marks->size; ++i) {
		kv_address = marks->valid_pairs[i];
		pointer_to_kv_in_log = kv_address;
		//struct splice *key = (struct splice *)kv_address;
		//struct splice *value = (struct splice *)(kv_address + VALUE_SIZE_OFFSET(key->size));
		handle.volume_desc = volume_desc;
		handle.db_desc = db_desc;
		ins_req.metadata.handle = &handle;
		ins_req.key_value_buf = kv_address;
		ins_req.metadata.append_to_log = 1;
		ins_req.metadata.gc_request = 1;
		ins_req.metadata.recovery_request = 0;
		_insert_key_value(&ins_req);
		//update_key_value_pointer(&handle, key->data, value->data, key->size, value->size);
	}
}

int8_t find_deleted_kv_pairs_in_segment(volume_descriptor *volume_desc, db_descriptor *db_desc, char *logsegment,
					stack *marks)
{
	struct db_handle handle = { .volume_desc = volume_desc, .db_desc = db_desc };
	struct kv_format *key;
	struct value_format *value;
	void *value_as_pointer;
	void *find_value;
	char *start_of_log_segment = logsegment;
	uint64_t size_of_log_segment_checked = 0;
	uint64_t log_data_without_metadata = LOG_DATA_OFFSET;
	uint64_t remaining_space;
	int key_value_size;
	int garbage_collect_segment = 0;

	key = (struct kv_format *)logsegment;
	key_value_size = sizeof(key->key_size) * 2;
	marks->size = 0;
	remaining_space = LOG_DATA_OFFSET - (uint64_t)(logsegment - start_of_log_segment);

	while (size_of_log_segment_checked < log_data_without_metadata && key->key_size != 0 && remaining_space >= 10) {
		key = (struct kv_format *)logsegment;
		value = (struct value_format *)(logsegment + VALUE_SIZE_OFFSET(key->key_size));
		value_as_pointer = (logsegment + VALUE_SIZE_OFFSET(key->key_size));
		find_value = find_key(&handle, key->key_buf, key->key_size);

		if (key->key_size != 0 && remaining_space >= 10 &&
		    (find_value == NULL || value_as_pointer != find_value)) {
			garbage_collect_segment = 1;
		} else if (key->key_size != 0 && remaining_space >= 10 &&
			   (find_value != NULL && value_as_pointer == find_value)) {
			push_stack(marks, logsegment);
		}

		if (key->key_size != 0 && remaining_space >= 10) {
			logsegment += key->key_size + value->value_size + key_value_size;
			size_of_log_segment_checked += key->key_size + value->value_size + key_value_size;
			remaining_space = LOG_DATA_OFFSET - (uint64_t)(logsegment - start_of_log_segment);
		} else
			break;
	}

	assert(marks->size < STACK_SIZE);
	if (garbage_collect_segment) {
		move_kv_pairs_to_new_segment(volume_desc, db_desc, marks);
		return 1;
	}
	return 0;
}

void fix_nodes_in_log(volume_descriptor *volume_desc, db_descriptor *db_desc, log_segment *prev_node,
		      log_segment *curr_node)
{
	if (prev_node) {
		prev_node->metadata.next_segment = curr_node->metadata.next_segment;
		free_block(volume_desc, curr_node, SEGMENT_SIZE);
	} else
		db_desc->KV_log_first_segment =
			(segment_header *)REAL_ADDRESS((uint64_t)curr_node->metadata.next_segment);
}

void iterate_log_segments(db_descriptor *db_desc, volume_descriptor *volume_desc, stack *marks)
{
	log_segment *last_segment = (log_segment *)db_desc->KV_log_last_segment;
	log_segment *log_node = (log_segment *)db_desc->KV_log_first_segment;
	log_segment *prev_node = NULL;

	/* We are in the first segment of the log and is not yet full! */
	if (log_node->metadata.next_segment == NULL) {
		log_debug("We reached at the last log segment");
		return;
	}

	while (REAL_ADDRESS(log_node->metadata.next_segment) != last_segment) {
		int8_t ret = find_deleted_kv_pairs_in_segment(volume_desc, db_desc, log_node->data, marks);

		if (ret == 1)
			fix_nodes_in_log(volume_desc, db_desc, prev_node, log_node);

		prev_node = log_node;
		log_node = (log_segment *)REAL_ADDRESS((uint64_t)log_node->metadata.next_segment);
	}

	/* The log had multiple nodes and we reached at the last one! */
	if (REAL_ADDRESS(log_node->metadata.next_segment) == last_segment) {
		log_debug("We reached at the last log segment");
		return;
	}

	log_fatal("Log is corrupted!");
	assert(0);
}

void *gc_log_entries(void *v_desc)
{
	struct timespec ts;
	stack *marks;
	volume_descriptor *volume_desc = (volume_descriptor *)v_desc;
	db_descriptor *db_desc = NULL;
	struct klist_node *region;
	int rc;

	marks = malloc(sizeof(stack));
	if (!marks) {
		log_error("ERROR i could not allocate stack");
		exit(EXIT_FAILURE);
	}

	log_debug("Starting garbage collection thread");
	while (1) {
		if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
			perror("FATAL: clock_gettime failed)\n");
			exit(-1);
		}
		ts.tv_sec += (GC_INTERVAL / 1000000L);
		ts.tv_nsec += (GC_INTERVAL % 1000000L) * 1000L;

		MUTEX_LOCK(&volume_desc->gc_mutex);
		rc = pthread_cond_timedwait(&volume_desc->gc_cond, &volume_desc->gc_mutex, &ts);
		MUTEX_UNLOCK(&volume_desc->gc_mutex);

		if (rc != 0 || rc != ETIMEDOUT) {
			log_debug("Error in GC thread");
			exit(EXIT_FAILURE);
		}

		log_debug("Initiating garbage collection");

		if (volume_desc->state == VOLUME_IS_CLOSING || volume_desc->state == VOLUME_IS_CLOSED) {
			log_debug("GC thread exiting %s", volume_desc->volume_id);
			free(marks);
			pthread_exit(NULL);
		}

		region = klist_get_first(volume_desc->open_databases);

		while (region != NULL) {
			db_desc = (db_descriptor *)region->data;
			iterate_log_segments(db_desc, volume_desc, marks);
			region = region->next;
		}
		log_debug("Garbage Collection Finished");
	}

	return NULL;
}

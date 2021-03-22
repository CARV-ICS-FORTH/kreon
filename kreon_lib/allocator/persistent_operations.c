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

#include <sys/mman.h>
#include <errno.h>
#include "allocator.h"
#include "../btree/btree.h"
#include "../btree/segment_allocator.h"
#include <assert.h>
#include <log.h>

#if 0
/*persists the KV-log of a DB, not thread safe!*/
void commit_db_log(db_descriptor *db_desc, commit_log_info *info)
{
	segment_header *current_segment;
	segment_header *first;
	segment_header *last;

	/*sync data then metadata*/
	first = (segment_header *)(MAPPED + (uint64_t)db_desc->commit_log->last_kv_log);
	last = info->last_kv_log;
	current_segment = first;

	while (1) {
		msync(current_segment, (size_t)SEGMENT_SIZE, MS_SYNC);
		if (current_segment == last)
			break;
		current_segment = (segment_header *)(MAPPED + (uint64_t)current_segment->next_segment);
	}
	/*write log info*/
	if (info->first_kv_log != NULL)
		db_desc->commit_log->first_kv_log = (segment_header *)((uint64_t)info->first_kv_log - MAPPED);
	else
		db_desc->commit_log->first_kv_log = NULL;

	if (info->last_kv_log != NULL)
		db_desc->commit_log->last_kv_log = (segment_header *)((uint64_t)info->last_kv_log - MAPPED);
	else
		db_desc->commit_log->last_kv_log = NULL;

	db_desc->commit_log->kv_log_size = info->kv_log_size;

	if (msync(db_desc->commit_log, sizeof(commit_log_info), MS_SYNC) == -1) {
		log_fatal("msync failed");
		exit(EXIT_FAILURE);
	}
	return;
}

void commit_db_logs_per_volume(volume_descriptor *volume_desc)
{
	struct commit_log_info info;
	NODE *node;
	db_descriptor *db_desc;
	node = get_first(volume_desc->open_databases);

	while (node != NULL) {
		db_desc = (db_descriptor *)(node->data);
		/*stop level 0 writers for this db*/
		RWLOCK_WRLOCK(&db_desc->levels[0].guard_of_level.rx_lock);
		/*spinning*/
		spin_loop(&(db_desc->levels[0].active_writers), 0);

		MUTEX_LOCK(&db_desc->lock_log);
		info.first_kv_log = (segment_header *)db_desc->KV_log_first_segment;
		info.last_kv_log = (segment_header *)db_desc->KV_log_last_segment;
		info.kv_log_size = db_desc->KV_log_size;

		MUTEX_UNLOCK(&db_desc->lock_log);
		RWLOCK_UNLOCK(&db_desc->levels[0].guard_of_level.rx_lock);

		if (db_desc->commit_log->kv_log_size != db_desc->KV_log_size)
			commit_db_log(db_desc, &info);
		node = node->next;
	}
}
#endif

/*As normal snapshot except it increases system's epoch
 * even in the case where no writes have taken place
 */
void force_snapshot(volume_descriptor *volume_desc)
{
	volume_desc->force_snapshot = 1;
	snapshot(volume_desc);
}

static void stop_readers_writers(struct volume_descriptor *volume_desc)
{
	struct klist_node *node = klist_get_first(volume_desc->open_databases);
	while (node != NULL) {
		struct db_descriptor *db_desc = (struct db_descriptor *)(node->data);
		/*stop all writers clients and compaction threads*/
		for (int level_id = 0; level_id < MAX_LEVELS; level_id++) {
			RWLOCK_WRLOCK(&db_desc->levels[level_id].guard_of_level.rx_lock);
			/*spinning*/
			spin_loop(&(db_desc->levels[level_id].active_writers), 0);
		}
		node = node->next;
	}
	//all dbs locked
	//Acquire locks of the cleaner
	MUTEX_LOCK(&volume_desc->free_log_lock);
	MUTEX_LOCK(&volume_desc->bitmap_lock);
}

static void resume_readers_writers(struct volume_descriptor *volume_desc)
{
	struct klist_node *node = klist_get_first(volume_desc->open_databases);
	while (node != NULL) {
		struct db_descriptor *db_desc = (struct db_descriptor *)(node->data);
		for (int level_id = 0; level_id < MAX_LEVELS; level_id++) {
			RWLOCK_UNLOCK(&db_desc->levels[level_id].guard_of_level.rx_lock);
			spin_loop(&(db_desc->levels[level_id].active_writers), 0);
		}
		node = node->next;
	}
	//all dbs locked
	//Acquire locks of the cleaner
	MUTEX_UNLOCK(&volume_desc->free_log_lock);
	MUTEX_UNLOCK(&volume_desc->bitmap_lock);
}

/*persists a consistent snapshot of the system*/
void snapshot(volume_descriptor *volume_desc)
{
	pr_db_group *db_group;
	pr_db_entry *db_entry;
	int32_t dirty = 0;

	log_info("Trigerring Snapshot");
	volume_desc->snap_preemption = SNAP_INTERRUPT_ENABLE;

	stop_readers_writers(volume_desc);
	if (volume_desc->force_snapshot) {
		dirty = 1;
		volume_desc->force_snapshot = 0;
	}

	//Iterate dbs
	struct klist_node *node = klist_get_first(volume_desc->open_databases);
	while (node != NULL) {
		struct db_descriptor *db_desc = (struct db_descriptor *)node->data;

		dirty += db_desc->dirty;
		/*update the catalogue if db is dirty*/
		if (db_desc->dirty > 0) {
			db_desc->dirty = 0x00;
			/*cow check*/
			db_group =
				(pr_db_group *)(MAPPED +
						(uint64_t)volume_desc->mem_catalogue->db_group_index[db_desc->group_id]);

			// log_info("group epoch %llu  dev_catalogue %llu",
			// (LLU)db_group->epoch,
			// volume_desc->dev_catalogue->epoch);
			if (db_group->epoch <= volume_desc->dev_catalogue->epoch) {
				// log_info("cow for db_group %llu", (LLU)db_group);
				/*do cow*/
				// superindex_db_group * new_group = (superindex_db_group
				// *)allocate(volume_desc,DEVICE_BLOCK_SIZE,-1,GROUP_COW);
				pr_db_group *new_group =
					(pr_db_group *)get_space_for_system(volume_desc, sizeof(pr_db_group), 0);

				memcpy(new_group, db_group, sizeof(pr_db_group));
				new_group->epoch = volume_desc->mem_catalogue->epoch;
				free_system_space(volume_desc, db_group, sizeof(pr_db_group));
				db_group = new_group;
				volume_desc->mem_catalogue->db_group_index[db_desc->group_id] =
					(pr_db_group *)((uint64_t)db_group - MAPPED);
			}

			db_entry = &(db_group->db_entries[db_desc->group_index]);
			strcpy(db_entry->db_name, db_desc->db_name);
			// log_info("pr db entry name %s db name %s", db_entry->db_name,
			// db_desc->db_name);

			for (int levelid = 1; levelid < MAX_LEVELS; levelid++) {
				for (int tree_id = 0; tree_id < NUM_TREES_PER_LEVEL; tree_id++) {
					/*Serialize and persist space allocation info for all levels*/
					if (db_desc->levels[levelid].last_segment[tree_id] != NULL) {
						db_entry->first_segment[levelid][tree_id] =
							(uint64_t)db_desc->levels[levelid].first_segment[tree_id] -
							MAPPED;
						db_entry->last_segment[levelid][tree_id] =
							(uint64_t)db_desc->levels[levelid].last_segment[tree_id] -
							MAPPED;
						db_entry->offset[levelid][tree_id] =
							(uint64_t)db_desc->levels[levelid].offset[tree_id];
					} else {
						db_entry->first_segment[levelid][tree_id] = 0;
						db_entry->last_segment[levelid][tree_id] = 0;
						db_entry->offset[levelid][tree_id] = 0;
					}

					/*now mark new roots*/
					if (db_desc->levels[levelid].root_w[tree_id] != NULL) {
						db_entry->root_r[levelid][tree_id] =
							((uint64_t)db_desc->levels[levelid].root_w[tree_id]) - MAPPED;

						/*mark old root to free it later*/
						db_desc->levels[levelid].root_r[tree_id] =
							db_desc->levels[levelid].root_w[tree_id];
						db_desc->levels[levelid].root_w[tree_id] = NULL;
					} else if (db_desc->levels[levelid].root_r[tree_id] != NULL) {
						// log_warn("set %lu to %llu of db_entry %llu", i * j,
						//	 db_entry->root_r[(i * MAX_LEVELS) + j],
						//(uint64_t)db_entry - MAPPED);
						db_entry->root_r[levelid][tree_id] =
							((uint64_t)db_desc->levels[levelid].root_r[tree_id]) - MAPPED;
					} else {
						db_entry->root_r[levelid][tree_id] = 0;
					}

					db_entry->level_size[levelid][tree_id] =
						db_desc->levels[levelid].level_size[tree_id];
				}
			}
			/*KV log status*/
			if (db_desc->KV_log_first_segment != NULL) {
				db_entry->KV_log_first_seg_offt = (uint64_t)db_desc->KV_log_first_segment - MAPPED;
				db_entry->KV_log_last_seg_offt = (uint64_t)db_desc->KV_log_last_segment - MAPPED;
				db_entry->KV_log_size = (uint64_t)db_desc->KV_log_size;
			} else {
				db_entry->KV_log_first_seg_offt = 0;
				db_entry->KV_log_last_seg_offt = 0;
				db_entry->KV_log_size = 0;
			}
			db_entry->L1_index_end_log_offset = db_desc->L1_index_end_log_offset;
			if (db_desc->L1_segment != NULL)
				db_entry->L1_segment_offt = (uint64_t)db_desc->L1_segment - MAPPED;
			else
				db_entry->L1_segment_offt = 0;
#if 0
			db_entry->commit_log = (uint64_t)db_desc->commit_log - MAPPED;
			log_info.first_kv_log = (segment_header *)db_desc->KV_log_first_segment;
			log_info.last_kv_log = (segment_header *)db_desc->KV_log_last_segment;
			log_info.kv_log_size = db_desc->KV_log_size;
			commit_db_log(db_desc, &log_info);
#endif
		}
		node = node->next;
	}
	if (dirty > 0) {
		/*At least one db is dirty proceed to snapshot()*/
		free_system_space(volume_desc, volume_desc->dev_catalogue, sizeof(pr_system_catalogue));
		volume_desc->dev_catalogue = volume_desc->mem_catalogue;
		/*allocate a new position for superindex*/

		pr_system_catalogue *tmp =
			(pr_system_catalogue *)get_space_for_system(volume_desc, sizeof(pr_system_catalogue), 0);
		memcpy(tmp, volume_desc->dev_catalogue, sizeof(pr_system_catalogue));
		++tmp->epoch;
		volume_desc->mem_catalogue = tmp;
		bitmap_set_buddies_immutable(volume_desc);
	}

	volume_desc->last_snapshot = get_timestamp(); /*update snapshot ts*/
	volume_desc->last_commit = volume_desc->last_snapshot;
	volume_desc->last_sync = get_timestamp(); /*update snapshot ts*/

	if (dirty > 0) {
		/*At least one db is dirty proceed to snapshot()*/
		log_info("Syncing volume... from %llu to %llu", volume_desc->start_addr, volume_desc->size);
		if (msync(volume_desc->start_addr, volume_desc->size, MS_SYNC) != 0) {
			log_fatal("Error at msync start_addr %llu size %llu", (LLU)volume_desc->start_addr,
				  (LLU)volume_desc->size);
			switch (errno) {
			case EBUSY:
				log_error("msync returned EBUSY");
				break;
			case EINVAL:
				log_error("msync returned EINVAL");
				break;
			case ENOMEM:
				log_error("msync returned EBUSY");
				break;
			}
			exit(EXIT_FAILURE);
		}
	}
	/*Write superblock*/
	volume_desc->volume_superblock->system_catalogue =
		(pr_system_catalogue *)((uint64_t)volume_desc->dev_catalogue - MAPPED);

	resume_readers_writers(volume_desc);
	volume_desc->snap_preemption = SNAP_INTERRUPT_DISABLE;
}

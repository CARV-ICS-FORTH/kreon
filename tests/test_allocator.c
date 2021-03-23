
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
#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <assert.h>
#include <log.h>
#include "../kreon_lib/allocator/allocator.h"
#include "../kreon_lib/btree/btree.h"

static struct volume_descriptor *open_volume(char *volumeName)
{
	int val = 0;
	int digits = 0;
	while (val > 0) {
		val = val / 10;
		digits++;
	}
	if (digits == 0)
		digits = 1;

	char *key = calloc(1, strlen(volumeName) + digits + 1);
	if (!key) {
		log_fatal("Calloc failed");
		exit(EXIT_FAILURE);
	}
	strcpy(key, volumeName);
	sprintf(key + strlen(volumeName), "%llu", (LLU)0);
	key[strlen(volumeName) + digits] = '\0';
	struct volume_descriptor *volume_desc = calloc(1, sizeof(volume_descriptor));
	if (!volume_desc) {
		log_fatal("Calloc failed");
		exit(EXIT_FAILURE);
	}
	volume_desc->state = VOLUME_IS_OPEN;
	volume_desc->snap_preemption = SNAP_INTERRUPT_DISABLE;
	volume_desc->last_snapshot = get_timestamp();
	volume_desc->last_commit = get_timestamp();
	volume_desc->last_sync = get_timestamp();

	volume_desc->volume_name = calloc(1, strlen(volumeName) + 1);
	if (!volume_desc->volume_name) {
		log_fatal("Calloc failed");
		exit(EXIT_FAILURE);
	}
	strcpy(volume_desc->volume_name, volumeName);
	volume_desc->volume_id = malloc(strlen(key) + 1);
	strcpy(volume_desc->volume_id, key);
	volume_desc->open_databases = NULL;
	volume_desc->offset = 0;
	/*allocator lock*/
	MUTEX_INIT(&(volume_desc->bitmap_lock), NULL);
	/*free operations log*/
	MUTEX_INIT(&(volume_desc->free_log_lock), NULL);
	// this call will fill volume's size
	allocator_init(volume_desc);
	log_info("Opened volume %s successfully", volume_desc->volume_name);
	return volume_desc;
}

static void free_device(struct volume_descriptor *volume_desc, uint64_t capacity)
{
	uint64_t bytes_freed = 0;
	uint64_t num_free_ops = 0;
	uint64_t diff = (uint64_t)volume_desc->bitmap_end - MAPPED;
	uint32_t pad = SEGMENT_SIZE - (diff % SEGMENT_SIZE);
	void *next_addr = volume_desc->bitmap_end + pad + SEGMENT_SIZE;
	int m_exit = 0;
	log_info("Freeing device %s", volume_desc->volume_name);
	while (!m_exit) {
		uint32_t num_bytes = (rand() % 67) * 4096;
		if (num_bytes == 0)
			continue;
		if (capacity - bytes_freed < num_bytes) {
			num_bytes = capacity - bytes_freed;
			m_exit = 1;
			log_info("This is the last free");
		}
		if (++num_free_ops % 10000 == 0) {
			log_info("Freed up to %llu out of %llu", bytes_freed, capacity);
		}
		free_block(volume_desc, next_addr, num_bytes);
		bytes_freed += num_bytes;
		++num_free_ops;
		next_addr += num_bytes;
	}
	if (bytes_freed != capacity) {
		log_fatal("Missing free bytes freed %llu capacity %llu", bytes_freed, capacity);
		exit(EXIT_FAILURE);
	}
	log_info("Freed all the %llu bytes of the device", bytes_freed);
	return;
}

int main(int argc, char **argv)
{
	if (argc != 3) {
		log_fatal("Wrong arguments, ./test_allocator <volume name> <volume size>");
		exit(EXIT_FAILURE);
	}

	char *ptr;
	long size = strtol(argv[2], &ptr, 10);
	volume_init(argv[1], 0, size, 1);
	struct volume_descriptor *volume_desc = open_volume(argv[1]);

	// calculate also padded bytes
	uint64_t diff = (uint64_t)volume_desc->bitmap_end - MAPPED;
	uint32_t pad = SEGMENT_SIZE - (diff % SEGMENT_SIZE);
	log_info("Calculated %llu bytes used for alignment purposes adding one "
		 "segment also for system's catalgue accounting",
		 (LLU)pad);
	uint64_t metadata = ((uint64_t)volume_desc->bitmap_end - MAPPED) + pad + SEGMENT_SIZE;
	uint64_t unmapped_bytes = volume_desc->volume_superblock->unmapped_blocks * DEVICE_BLOCK_SIZE;

	uint64_t device_capacity = size - (metadata + unmapped_bytes);

	for (int i = 0; i < 2; ++i) {
		uint64_t bytes_allocated = 0;
		uint64_t num_allocations = 0;
		void *next_addr = volume_desc->bitmap_end;
		next_addr = next_addr + pad + SEGMENT_SIZE;
		int last = 0;
		if (i == 0)
			log_info("Testing small allocations");
		else
			log_info("Testing segment allocations");

		while (1) {
			uint32_t num_bytes;
			if (i == 0) {
				num_bytes = (rand() % 67) * 4096;
				if (num_bytes == 0)
					continue;
			} else
				num_bytes = 2 * 1024 * 1024;

			if (++num_allocations % 10000 == 0)
				log_info("Have allocated %llu bytes so far out of device capacity %llu",
					 bytes_allocated, device_capacity);

			if (device_capacity - bytes_allocated < num_bytes) {
				num_bytes = device_capacity - bytes_allocated;
				last = 1;
				log_info("Trying to allocate the last %u bytes", num_bytes);
			}
			void *addr = allocate(volume_desc, num_bytes, 0, 0);
			if (addr == NULL) {
				log_fatal("Device out of space thish should not happen!");
				exit(EXIT_FAILURE);
			}
			if ((uint64_t)addr != (uint64_t)next_addr) {
				log_fatal("Allocation failed for num bytes %u should have been %llu got "
					  "%llu diff %llu total bytes allocated so far %llu",
					  num_bytes, next_addr, addr, (uint64_t)addr - (uint64_t)next_addr,
					  bytes_allocated);
				exit(EXIT_FAILURE);
			}
			if (i > 0) {
				if (((uint64_t)addr - MAPPED) % (2 * 1024 * 1024) != 0) {
					log_fatal("Addr is %llu, offt %llu MAPPED %llu", addr, (uint64_t)addr - MAPPED,
						  MAPPED);
					exit(EXIT_FAILURE);
				}
			}

			bytes_allocated += num_bytes;
			if (last)
				break;
			next_addr = addr + num_bytes;
		}
		// sanity check everything is allocated
		void *addr = allocate(volume_desc, 4096, 0, 0);
		if (addr != NULL) {
			log_fatal("Whole device should have been allocated at this step");
			exit(EXIT_FAILURE);
		}

		if (bytes_allocated != device_capacity) {
			log_fatal("Managed to allocate %llu bytes when device capacity is %llu bytes", bytes_allocated,
				  device_capacity);
			exit(EXIT_FAILURE);
		}
		log_info("ALLOCATION test successfull round %d! freeing everything", i);
		free_device(volume_desc, device_capacity);
		log_info("ALL free done");
		force_snapshot(volume_desc);
		while (volume_desc->mem_catalogue->free_log_position !=
		       volume_desc->mem_catalogue->free_log_last_free) {
			sleep(4);
			log_info("Waiting for cleaner to do its job free log pos %llu last free "
				 "pos %llu",
				 volume_desc->mem_catalogue->free_log_position,
				 volume_desc->mem_catalogue->free_log_last_free);
		}
		log_info("Proceeding to next round!");
	}

	log_info("ALL tests successfull");
	return 1;
}

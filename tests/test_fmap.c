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
#define DEV_GROUP_SIZE 4

static void write_magic_key(uint64_t dev_offt, int magic_key, int myfd)
{
	int *IO_buf = NULL;
	if (posix_memalign((void **)&IO_buf, DEVICE_BLOCK_SIZE, DEVICE_BLOCK_SIZE) != 0) {
		log_fatal("Posix memalign failed");
		perror("Reason: ");
		exit(EXIT_FAILURE);
	}

	IO_buf[0] = magic_key;
	ssize_t total_bytes_written = 0;
	ssize_t bytes_written = 0;
	ssize_t size = DEVICE_BLOCK_SIZE;
	while (total_bytes_written < size) {
		bytes_written = pwrite(myfd, IO_buf, size - total_bytes_written, dev_offt + total_bytes_written);
		if (bytes_written == -1) {
			log_fatal("Failed to writed segment for leaf nodes reason follows dev_offt %llu", dev_offt);
			perror("Reason");
			exit(EXIT_FAILURE);
		}
		total_bytes_written += bytes_written;
	}
	if (fsync(myfd)) {
		log_fatal("Failed to sync file");
		exit(EXIT_FAILURE);
	}
	free(IO_buf);
	return;
}

static void fmap_free_device(struct volume_descriptor *volume_desc, uint64_t capacity, uint64_t start_offt)
{
	uint64_t bytes_freed = 0;
	uint64_t num_free_ops = 0;
	void *next_addr = (void *)(MAPPED + start_offt);
	int m_exit = 0;
	log_info("Freeing device %s", volume_desc->volume_name);
	while (!m_exit) {
		uint32_t num_bytes = SEGMENT_SIZE;
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
	log_info("Freed all the %llu bytes of the device, waiting the cleaner to do its job", bytes_freed);
	force_snapshot(volume_desc);
	while (volume_desc->mem_catalogue->free_log_position != volume_desc->mem_catalogue->free_log_last_free) {
		sleep(4);
		log_info("Waiting for cleaner to do its job free log pos %llu last free "
			 "pos %llu",
			 volume_desc->mem_catalogue->free_log_position, volume_desc->mem_catalogue->free_log_last_free);
	}
	log_info("Device clean!");
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
	struct volume_descriptor *volume_desc = get_volume_desc("/dev/dmap/dmap1", 0, 1);
	int my_fd = open(argv[1], O_RDWR | O_DIRECT | O_SYNC);
	if (my_fd == -1) {
		log_fatal("Failed to open volume %s", argv[1]);
		perror("Reason:");
		exit(EXIT_FAILURE);
	}

	// calculate also padded bytes
	uint64_t diff = (uint64_t)volume_desc->bitmap_end - MAPPED;
	uint32_t pad = SEGMENT_SIZE - (diff % SEGMENT_SIZE);
	log_info("Calculated %llu bytes used for alignment purposes adding one "
		 "segment also for system's catalgue accounting",
		 (LLU)pad);
	uint64_t metadata = ((uint64_t)volume_desc->bitmap_end - MAPPED) + pad + SEGMENT_SIZE;
	uint64_t unmapped_bytes = volume_desc->volume_superblock->unmapped_blocks * DEVICE_BLOCK_SIZE;

	uint64_t device_capacity = size - (metadata + unmapped_bytes);
	device_capacity -= (pad + SEGMENT_SIZE);
	uint64_t group_size = (device_capacity / DEV_GROUP_SIZE);
	uint64_t dev_group[DEV_GROUP_SIZE];
	dev_group[0] = metadata + pad;
	for (int i = 1; i < DEV_GROUP_SIZE; i++)
		dev_group[i] = dev_group[i - 1] + group_size;

	//uint64_t num_IOs_per_dev_group = (group_size / 1000) / DEVICE_BLOCK_SIZE;
	uint64_t num_IOs_per_dev_group = 2000;

	log_info("Split groups in %llu groups of size %llu bytes "
		 "num_IOs_per_dev_group %llu",
		 DEV_GROUP_SIZE, group_size, num_IOs_per_dev_group);

	time_t t;
	/* Intializes random number generator */
	srand((unsigned)time(&t));
	for (int round = 0; round < 2; round++) {
		for (int i = 0; i < DEV_GROUP_SIZE; i++) {
			for (uint64_t n = 0; n < num_IOs_per_dev_group; n++) {
				uint64_t dev_offt = dev_group[i] + (n * DEVICE_BLOCK_SIZE);
				int magic_key = rand();
				write_magic_key(dev_offt, magic_key, my_fd);

				/*produce now the page fault*/
				int *addr = (int *)(MAPPED + dev_offt);
				if (*addr == magic_key) {
					log_fatal("Failure fastmap read the block! It shouldn't! magic key %d",
						  magic_key);
					//exit(EXIT_FAILURE);
				}
				if (n % 1000 == 0)
					log_info("Success up to %d IOs for group %d", n, i);
			}
		}
		if (round == 0) {
			/*Test the padded area this should pass*/
			uint64_t dev_offt = (uint64_t)volume_desc->bitmap_end - MAPPED;
			int magic_key = rand();
			write_magic_key(dev_offt, magic_key, my_fd);
			/*produce now the page fault*/
			int *addr = (int *)(MAPPED + dev_offt);
			if (*addr != magic_key) {
				log_fatal(
					"Failure fastmap didn't read the block! It should! magic key %d however read %d",
					magic_key, *addr);
				exit(EXIT_FAILURE);
			}
			log_info("Success read magic key %d in paddedSpace !", magic_key);
		}
		/*Now allocate and free all the device and retry*/
		uint64_t remaining_bytes = device_capacity;
		void *m_addr;
		while (1) {
			uint32_t alloc_bytes;
			if (remaining_bytes > SEGMENT_SIZE)
				alloc_bytes = SEGMENT_SIZE;
			else
				alloc_bytes = remaining_bytes;
			m_addr = allocate(volume_desc, alloc_bytes, 0, 0);
			if (m_addr != NULL)
				remaining_bytes -= alloc_bytes;
			else {
				if (remaining_bytes != 0) {
					log_fatal(
						"Failed to allocate all the device capacity %llu unallocated bytes %llu",
						device_capacity, remaining_bytes);
				}
				log_info("Done allocated all the device");
				break;
			}
		}
		fmap_free_device(volume_desc, device_capacity, metadata + pad + SEGMENT_SIZE);
	}
	log_info("ALL tests successfull");
	if (close(my_fd) == -1) {
		log_fatal("Failed to close volume %s", argv[1]);
		exit(EXIT_FAILURE);
	}
	return 1;
}

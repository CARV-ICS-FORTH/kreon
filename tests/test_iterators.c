#include <assert.h>
#include <stdio.h>
#include <alloca.h>
#include <string.h>
#include <stdlib.h>
#include <log.h>
#include <kreon.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define KEY_PREFIX "userakias_computerakias"
#define KV_SIZE 1024
#define NUM_KEYS 1000000
#define NUM_OF_ROUNDS 1
#define BASE 1000000

void test_iterators(klc_handle hd)
{
	struct klc_key_value kv;
	uint64_t i = 0;
	uint64_t j = 0;
	kv.k.data = (char *)malloc(KV_SIZE);
	if (kv.k.data == NULL) {
		log_fatal("Malloc failed");
		exit(EXIT_FAILURE);
	}
	kv.v.data = (char *)malloc(KV_SIZE);
	if (kv.v.data == NULL) {
		log_fatal("Malloc failed");
		exit(EXIT_FAILURE);
	}

	for (int round = 0; round < NUM_OF_ROUNDS; ++round) {
		log_info("Round %d Starting population for %lu keys...", round, NUM_KEYS);
		int local_base = BASE + (round * NUM_KEYS);
		for (i = local_base; i < local_base + NUM_KEYS; i++) {
			memcpy((char *)kv.k.data, KEY_PREFIX, strlen(KEY_PREFIX));
			sprintf((char *)kv.k.data + strlen(KEY_PREFIX), "%llu", (long long unsigned)i);
			kv.k.size = strlen(kv.k.data) + 1;
			kv.v.size = KV_SIZE;
			memset((char *)kv.v.data, 0XDD, kv.v.size);
			if (klc_put(hd, &kv) != KLC_SUCCESS) {
				log_fatal("Put failed");
				exit(EXIT_FAILURE);
			}
		}
	}
	log_info("Population ended, snapshot and testing iterators");
	klc_sync(hd);

	//scan the db serially
	klc_scanner s = klc_init_scanner(hd, NULL, KLC_FETCH_FIRST);
	assert(klc_is_valid(s));
	int64_t scan_size = NUM_KEYS;

	for (int64_t j = 1; j < scan_size; j++) {
		memcpy((char *)kv.k.data, KEY_PREFIX, strlen(KEY_PREFIX));
		sprintf((char *)kv.k.data + strlen(KEY_PREFIX), "%llu", (long long unsigned)BASE + j);
		kv.k.size = strlen(kv.k.data) + 1;
		if (klc_get_next(s) && !klc_is_valid(s)) {
			log_fatal("DB end at key %s is this correct? NO", kv.k.data);
			exit(EXIT_FAILURE);
		}
		struct klc_key keyptr = klc_get_key(s);

		if (kv.k.size != keyptr.size || memcmp(kv.k.data, keyptr.data, kv.k.size) != 0) {
			log_fatal("Test failed key %s not found scanner instead returned %s", kv.k.data, keyptr);
		}
	}

	log_info("Found keys serially\n");
	klc_close_scanner(s);
	//scan the db reversally
	//klc_close_scanner(s);
	klc_scanner s2 = klc_init_scanner(hd, NULL, KLC_FETCH_LAST);
	assert(klc_is_valid(s2));
	scan_size = NUM_KEYS;

	//cant declare j as unsinged because of wrap arround
	for (int64_t j = scan_size - 2; j >= 0; j--) {
		memcpy((char *)kv.k.data, KEY_PREFIX, strlen(KEY_PREFIX));
		sprintf((char *)kv.k.data + strlen(KEY_PREFIX), "%llu", (long long unsigned)BASE + j);
		kv.k.size = strlen(kv.k.data) + 1;
		if (klc_get_prev(s2) && !klc_is_valid(s2)) {
			log_fatal("DB end at key %s is this correct? NO", kv.k.data);
			exit(EXIT_FAILURE);
		}

		struct klc_key keyptr = klc_get_key(s2);
		if (kv.k.size != keyptr.size || memcmp(kv.k.data, keyptr.data, kv.k.size) != 0) {
			log_fatal("Test failed key %s not found scanner instead returned %s", kv.k.data, keyptr);
		}
	}
	klc_close_scanner(s2);
	log_info("found keys reversally, ending iterators test");
	free((char *)kv.k.data);
	free((char *)kv.v.data);
}

int main(int argc, char **argv)
{
	char *db_name = "test_iterators";

	klc_db_options db_options;

	int64_t size;
	int fd = open(argv[1], O_RDWR);
	if (fd == -1) {
		perror("open");
		exit(EXIT_FAILURE);
	}
	size = lseek(fd, 0, SEEK_END);
	if (size == -1) {
		log_fatal("failed to determine file size exiting...");
		perror("ioctl");
		exit(EXIT_FAILURE);
	}

	close(fd);
	log_info("Size is %lld", size);
	volume_init(argv[1], 0, size, 1);
	db_options.volume_size = size;

	db_options.volume_start = 0;
	db_options.volume_name = argv[1];

	db_options.create_flag = KLC_CREATE_DB;

	klc_handle hd;
	db_options.db_name = db_name;
	hd = klc_open(&db_options);

	test_iterators(hd);
}

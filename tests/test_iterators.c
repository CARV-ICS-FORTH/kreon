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
#define NUM_KEYS 10000000
#define SCAN_SIZE 16
#define NUM_OF_ROUNDS 1

void test_iterators(klc_handle hd)
{
	struct klc_key_value kv;
	uint64_t i = 0;
	uint64_t j = 0;
	kv.k.data = (char *)malloc(KV_SIZE);
	kv.v.data = (char *)malloc(KV_SIZE);

	for (int round = 0; round < NUM_OF_ROUNDS; ++round) {
		log_info("Round %d Starting population for %lu keys...", round, NUM_KEYS);
		for (i = 0; i < NUM_KEYS; i++) {
			memcpy((char *)kv.k.data, KEY_PREFIX, strlen(KEY_PREFIX));
			sprintf((char *)kv.k.data + strlen(KEY_PREFIX), "%llu", (long long unsigned)(i));
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

	uint64_t count = 0;
	//scan the db serially
	klc_scanner s = klc_init_scanner(hd, NULL, KLC_FETCH_FIRST);
	assert(klc_is_valid(s));
	count = 1;

	while (klc_get_next(s))
		count++;

	assert(count == NUM_KEYS);
	log_info("Found all keys serially\n");
	//scan the db reversally
	count = 0;
	s = klc_init_scanner(hd, NULL, KLC_FETCH_LAST);
	assert(klc_is_valid(s));
	count = 1;

	while (klc_get_prev(s))
		count++;

	klc_close_scanner(s);
	assert(count == NUM_KEYS);
	log_info("Found all keys reversally\n");
	// Seek for the middle key (could be random)
	struct klc_key k;
	k.data = (char *)malloc(KV_SIZE);
	memcpy((char *)k.data, KEY_PREFIX, strlen(KEY_PREFIX));
	sprintf((char *)k.data + strlen(KEY_PREFIX), "%llu", (long long unsigned)(NUM_KEYS / 2));
	k.size = strlen(k.data) + 1;
	klc_scanner s1 = klc_init_scanner(hd, NULL, KLC_FETCH_LAST);
	klc_seek(hd, &k, s1);

	struct klc_key keyptr = klc_get_key(s1);
	assert(!strcmp(k.data, keyptr.data));
	log_info("Found middle key (NUM_KEYS/2) -> %s", k.data);
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

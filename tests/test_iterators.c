#include <assert.h>
#include <stdio.h>
#include <alloca.h>
#include <string.h>
#include <stdlib.h>
#include <log.h>
#include "../kreon_lib/btree/btree.h"
#include "../kreon_lib/scanner/scanner.h"
#define KEY_PREFIX "userakias_computerakias"
#define KV_SIZE 1024

uint64_t num_keys;

typedef struct key {
	uint32_t key_size;
	char key_buf[];
} key;

typedef struct value {
	uint32_t value_size;
	char value_buf[0];
} value;

void insert_keys(db_handle *hd)
{
	bt_insert_req req;
	uint64_t i;
	key *k = (key *)alloca(KV_SIZE);

	log_info("Starting population for %lu keys...", num_keys);
	for (i = 0; i < num_keys; i++) {
		memcpy(k->key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
		sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)i);
		k->key_size = strlen(k->key_buf) + 1;
		value *v = (value *)((uint64_t)k + sizeof(key) + k->key_size);
		v->value_size = KV_SIZE - ((2 * sizeof(key)) + k->key_size);
		memset(v->value_buf, 0xDD, v->value_size);

		req.metadata.handle = hd;
		req.metadata.kv_size = k->key_size + v->value_size + (2 * sizeof(uint32_t));
		assert(req.metadata.kv_size == KV_SIZE);
		req.key_value_buf = k;
		req.metadata.level_id = 0;
		req.metadata.key_format = KV_FORMAT;
		req.metadata.append_to_log = 1;
		req.metadata.gc_request = 0;
		req.metadata.recovery_request = 0;
		_insert_key_value(&req);
	}
	log_info("Population ended");
}

void scan_db_serially(db_handle *hd, struct Kreoniterator *it)
{
	seek_to_first(hd, it);
	while (get_next(it) != END_OF_DATABASE)
		;
}

void scan_db_reversally(db_handle *hd, struct Kreoniterator *it)
{
	seek_to_last(hd, it);
	while (get_prev(it) != END_OF_DATABASE)
		;
}

void scan_db_serially_and_reversally(db_handle *hd, struct Kreoniterator *it)
{
	seek_to_first(hd, it);
	while (get_next(it) != END_OF_DATABASE)
		;

	seek_to_last(hd, it);
	while (get_prev(it) != END_OF_DATABASE)
		;
}

void seek_scan_test(db_handle *hd, void *keyname, struct Kreoniterator *it)
{
	Seek(hd, keyname, it);

	while (get_next(it) != END_OF_DATABASE)
		;
}

/* volume_name | number of keys*/
int main(int argc, char *argv[])
{
	num_keys = atol(argv[2]);
	char *volume_name = strdup(argv[1]);

	int64_t device_size;
	FD = open(volume_name, O_RDWR);
	if (ioctl(FD, BLKGETSIZE64, &device_size) == -1) {
		device_size = lseek(FD, 0, SEEK_END);
		if (device_size == -1) {
			log_fatal("failed to determine volume size exiting...");
			perror("ioctl");
			exit(EXIT_FAILURE);
		}
	}

	db_handle *hd = db_open(volume_name, 0, device_size, "test_iterators", CREATE_DB);

	insert_keys(hd);
	snapshot(hd->volume_desc);

	struct Kreoniterator *it = (struct Kreoniterator *)malloc(sizeof(struct Kreoniterator));

	scan_db_serially(hd, it);

	scan_db_reversally(hd, it);

	scan_db_serially_and_reversally(hd, it);

	//find key 72
	key *k = (key *)alloca(KV_SIZE);
	memcpy(k->key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
	sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)72);
	k->key_size = strlen(k->key_buf) + 1;
	value *v = (value *)((uint64_t)k + sizeof(key) + k->key_size);
	v->value_size = KV_SIZE - ((2 * sizeof(key)) + k->key_size);
	memset(v->value_buf, 0xDD, v->value_size);

	seek_scan_test(hd, (void *)k, it);

	//find key 81231

	memcpy(k->key_buf, KEY_PREFIX, strlen(KEY_PREFIX));
	sprintf(k->key_buf + strlen(KEY_PREFIX), "%llu", (long long unsigned)81231);
	k->key_size = strlen(k->key_buf) + 1;
	v = (value *)((uint64_t)k + sizeof(key) + k->key_size);
	v->value_size = KV_SIZE - ((2 * sizeof(key)) + k->key_size);
	memset(v->value_buf, 0xDD, v->value_size);

	seek_scan_test(hd, (void *)k, it);
}

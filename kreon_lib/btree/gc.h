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
#include "btree.h"
#include <assert.h>
#include <log.h>

typedef struct log_segment {
	segment_header metadata;
	char data[SEGMENT_SIZE - sizeof(segment_header)];
} log_segment;

/* The smallest entry in log that can exist  is a key and value of size 1.
 That means  the key + value size = 2 and the sizeof 2 integers = 8 */
#define STACK_SIZE ((SEGMENT_SIZE / 10) + 1)

typedef struct stack {
	void *valid_pairs[STACK_SIZE];
	int size;
} stack;

#define LOG_DATA_OFFSET (SEGMENT_SIZE - sizeof(segment_header))

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
#include <stdio.h>

typedef struct NODE {
	void *data;
	char *tag;
	/*function pointer for the custom destroy node function*/
	void (*destroy_node)(struct NODE *node);
	struct NODE *prev;
	struct NODE *next;
} NODE;

typedef struct LIST {
	NODE *first;
	NODE *last;
	int mode;
	int size;
} LIST;

LIST *init_list(void (*destroy_node)(NODE *node));
void add_first(LIST *list, void *data, const char *tag);
void add_last(LIST *list, void *data, const char *tag);
void *get_first(LIST *list);
void *find_element(LIST *list, char *key);
void *remove_first(LIST *list);
int remove_element(LIST *list, void *data);
void destroy_node(NODE *node);
void destroy_list(LIST *list);

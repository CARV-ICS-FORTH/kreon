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

struct segment_header *get_segment_for_explicit_IO(volume_descriptor *volume_desc, level_descriptor *level_desc,
						   uint8_t tree_id);
/*functions for index nodes*/
index_node *seg_get_index_node(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id,
			       char reason);

index_node *seg_get_index_node_header(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id,
				      char reason);

IN_log_header *seg_get_IN_log_block(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id,
				    char reason);

void seg_free_index_node_header(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id,
				node_header *node);

void seg_free_index_node(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id,
			 index_node *inode);

/*function for leaf nodes*/
leaf_node *seg_get_leaf_node(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id,
			     char reason);

leaf_node *seg_get_leaf_node_header(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id,
				    char reason);

void seg_free_leaf_node(volume_descriptor *volume_desc, level_descriptor *level_desc, uint8_t tree_id, leaf_node *leaf);

segment_header *seg_get_raw_index_segment(volume_descriptor *volume_desc, level_descriptor *level_desc, int tree_id);
/*log related*/
segment_header *seg_get_raw_log_segment(volume_descriptor *volume_desc);
void free_raw_segment(volume_descriptor *volume_desc, segment_header *segment);

void *get_space_for_system(volume_descriptor *volume_desc, uint32_t size, int lock);
void free_system_space(volume_descriptor *volume_desc, void *addr, uint32_t length);

void seg_free_level(db_handle *handle, uint8_t level_id, uint8_t tree_id);

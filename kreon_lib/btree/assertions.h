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

#define REBALANCE_CHECK                                                                                                \
	printf("DEBUG_REBALANCE: calling rebalance for my child %llu left bro %llu "                                   \
	       "right bro %llu, type %d\n",                                                                            \
	       child_addr, left_brother_addr, right_brother_addr, node->type);                                         \
	if (left_brother_addr == NULL && right_brother_addr == NULL) {                                                 \
		printf("FATAL both bro's null?\n");                                                                    \
		exit(-1);                                                                                              \
	}                                                                                                              \
	if (left_brother_addr == child_addr) {                                                                         \
		printf("FATAL, same with my left brother?\n");                                                         \
		exit(-1);                                                                                              \
	}                                                                                                              \
	if (right_brother_addr == child_addr) {                                                                        \
		printf("FATAL, same with my right brother?\n");                                                        \
		exit(-1);                                                                                              \
	}

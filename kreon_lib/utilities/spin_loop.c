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

#include <stdio.h>
#include <stdint.h>

int64_t get_counter(int64_t *counter);
uint32_t _read_value(uint32_t *value_addr);

void spin_loop(int64_t *counter, int64_t threashold)
{
	while (get_counter(counter) > threashold) {
	}
	return;
}

void wait_for_value(uint32_t *value_addr, uint32_t value)
{
	while (_read_value(value_addr) != value) {
	}
}

uint32_t _read_value(uint32_t *value_addr)
{
	return *value_addr;
}

int64_t get_counter(int64_t *counter)
{
	return *counter;
}

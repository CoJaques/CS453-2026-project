#pragma once

#include "tm.h"
#include "batcher.h"
#include <stdbool.h>

typedef enum { ACCESS_NONE, ACCESS_ONE, ACCESS_MANY } access_state_t;

typedef struct {
	pthread_mutex_t mutex;
	uint8_t read_copy; // Index of the readable copy: 0 or 1.
	bool written; // Whether the word was written during epoch.
	uint64_t epoch; // Epoch of the access and write metadata.
	access_state_t access_state; // Read-write transactions only.
	tx_t owner; // Meaningful only when access_state is ACCESS_ONE.
} word_status_t;

typedef struct segment_t {
	size_t size;
	void *data[2];
	word_status_t *status; // One control per word: size / region->align.
	struct segment_t *next;
} segment_t;

typedef struct {
	size_t size;
	size_t align;
	batcher_t *batcher;
	segment_t *head;
} region_t;

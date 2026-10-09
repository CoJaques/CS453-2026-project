#pragma once

#include "tm.h"
#include "batcher.h"
#include <stdbool.h>

typedef enum { ACCESS_NONE = 0, ACCESS_ONE, ACCESS_MANY } access_state_t;
enum { WORD_LOCK_COUNT = 1024 };

typedef struct {
	uint64_t epoch; // Epoch of the access and write metadata.
	tx_t owner; // Meaningful only when access_state is ACCESS_ONE.
	access_state_t access_state; // Read-write transactions only.
	bool written; // Whether the word was written during epoch.
} word_status_t;

typedef struct segment_t {
	size_t size;
	// Interior pointers: data[0] is committed, data[1] holds provisional writes.
	void *data[2];
	word_status_t *status; // Interior array of size / region->align controls.
	struct segment_t *next;
} segment_t;

typedef struct {
	segment_t *segment;
	size_t index; // Word index within the segment, not a byte offset.
} word_ref_t;

typedef struct segment_ref_t {
	segment_t *segment;
	struct segment_ref_t *next; // Independent of the region's segment list.
} segment_ref_t;

typedef struct transaction_t transaction_t;

typedef struct {
	size_t size;
	size_t align;
	unsigned int align_shift; // log2(align), computed once at region creation.
	batcher_t *batcher;
	segment_t *head;
	transaction_t *committed; // Protected by batcher->mutex; drain at epoch end.
	pthread_mutex_t word_locks[WORD_LOCK_COUNT]; // Protect word accesses, not whole transactions.
} region_t;

struct transaction_t {
	tx_t id;
	bool is_ro;
	uint64_t epoch;
	word_ref_t *written_words; // One entry per distinct word written.
	size_t written_count;
	size_t written_capacity;
	segment_ref_t *segment_allocated;
	segment_ref_t *segment_to_free;
	transaction_t *next_committed;
};

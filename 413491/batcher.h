#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
	pthread_mutex_t mutex;
	pthread_cond_t changed;
	uint64_t epoch;
	size_t remaining;
	size_t waiting;
} batcher_t;

/** Initialize a batcher before publishing it to other threads.
 * @param batcher Uninitialized batcher to initialize
 * @return Whether initialization succeeded
**/
bool batcher_init(batcher_t *batcher);

/** Destroy a batcher with no participants, waiting threads or concurrent calls.
 * @param batcher Initialized batcher to destroy
**/
void batcher_destroy(batcher_t *batcher);

/** [thread-safe] Enter the batcher, waiting for the next epoch if necessary.
 * Each successful entry must be paired with one leave. Threads must not be
 * cancelled while waiting or participating in a batch.
 * @param batcher Batcher associated with the shared memory region
**/
void batcher_enter(batcher_t *batcher);

/** [thread-safe] Leave the current batch, admitting the next batch if last.
 * @param batcher Batcher previously entered by the calling thread
**/
void batcher_leave(batcher_t *batcher);

/** [thread-safe] Get the epoch of the calling thread's current batch.
 * Only valid after enter returns and before the matching leave.
 * @param batcher Batcher currently entered by the calling thread
 * @return Current epoch number
**/
uint64_t batcher_get_epoch(batcher_t *batcher);

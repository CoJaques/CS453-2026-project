#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// Called with the batcher mutex held, before advancing the epoch or waking threads.
typedef void (*batcher_finalize_fn)(void *context);

typedef struct {
	pthread_mutex_t mutex;
	pthread_cond_t changed;
	uint64_t epoch;
	size_t remaining;
	size_t waiting;
	batcher_finalize_fn finalize_epoch;
	void *finalize_context;
} batcher_t;

/** Initialize a batcher before publishing it to other threads.
 * @param batcher Uninitialized batcher to initialize
 * @param finalize_epoch Optional callback, or NULL; must not reenter the batcher
 * @param context Callback context, valid until the batcher is destroyed
 * @return Whether initialization succeeded
**/
bool batcher_init(batcher_t *batcher, batcher_finalize_fn finalize_epoch,
		  void *context);

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
 * The last participant runs the finalization callback while holding the mutex.
 * @param batcher Batcher previously entered by the calling thread
**/
void batcher_leave(batcher_t *batcher);

/** [thread-safe] Get the epoch of the calling thread's current batch.
 * Only valid after enter returns and before the matching leave.
 * @param batcher Batcher currently entered by the calling thread
 * @return Current epoch number
**/
uint64_t batcher_get_epoch(batcher_t *batcher);

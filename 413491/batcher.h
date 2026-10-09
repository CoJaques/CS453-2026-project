#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>

#include "sync.h"

// Called with the batcher mutex held, before advancing the epoch or waking threads.
typedef void (*batcher_finalize_fn)(void *context);

typedef struct {
	pthread_mutex_t mutex;
	uint64_t epoch; // Protected by mutex; stable throughout each active batch.
	_Atomic uint32_t changed; // Publication signal and Linux futex word.
	size_t remaining; // Protected by mutex, including during finalization.
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
 * @return Epoch number of the batch entered
**/
uint64_t batcher_enter(batcher_t *batcher);

/** Acquire the batcher mutex before preparing a commit or leaving the batch.
 * @param batcher Batcher in which the caller is an active participant
**/
static inline void batcher_lock_for_leave(batcher_t *batcher)
{
	check_pthread(pthread_mutex_lock(&batcher->mutex));
}

/** Leave the current batch with the mutex held, admitting the next batch if last.
 * The last participant runs the finalization callback while holding the mutex.
 * The caller must first call batcher_lock_for_leave. This function releases
 * the mutex, including when this participant is not the last one.
 * @param batcher Batcher previously entered by the calling thread
**/
void batcher_leave_locked(batcher_t *batcher);

/** Acquire the mutex and leave the current batch, finalizing it if last.
 * The caller must not already hold the mutex.
 * @param batcher Batcher previously entered by the calling thread
**/
void batcher_leave(batcher_t *batcher);

#include "batcher.h"

#include <assert.h>
#include <stdlib.h>

/** Abort on a synchronization failure to avoid corrupting the batch state.
 * @param error Return code from a pthread operation
**/
static void check_pthread(int error)
{
	if (error != 0) {
		abort();
	}
}

bool batcher_init(batcher_t *batcher)
{
	if (pthread_mutex_init(&batcher->mutex, NULL) != 0) {
		return false;
	}
	if (pthread_cond_init(&batcher->changed, NULL) != 0) {
		check_pthread(pthread_mutex_destroy(&batcher->mutex));
		return false;
	}
	batcher->epoch = 0;
	batcher->remaining = 0;
	batcher->waiting = 0;
	return true;
}

void batcher_destroy(batcher_t *batcher)
{
	assert(batcher->remaining == 0 && batcher->waiting == 0);
	check_pthread(pthread_cond_destroy(&batcher->changed));
	check_pthread(pthread_mutex_destroy(&batcher->mutex));
}

void batcher_enter(batcher_t *batcher)
{
	check_pthread(pthread_mutex_lock(&batcher->mutex));
	if (batcher->remaining == 0) {
		batcher->remaining = 1;
	} else {
		uint64_t epoch = batcher->epoch;
		++batcher->waiting;
		while (batcher->epoch == epoch) {
			check_pthread(pthread_cond_wait(&batcher->changed,
						&batcher->mutex));
		}
		// The last leaver already counted us in the new batch.
	}
	check_pthread(pthread_mutex_unlock(&batcher->mutex));
}

void batcher_leave(batcher_t *batcher)
{
	check_pthread(pthread_mutex_lock(&batcher->mutex));
	assert(batcher->remaining > 0);
	if (--batcher->remaining == 0) {
		// STM epoch-finalization must run here, before admitting a new batch.
		++batcher->epoch;
		batcher->remaining = batcher->waiting;
		batcher->waiting = 0;
		check_pthread(pthread_cond_broadcast(&batcher->changed));
	}
	check_pthread(pthread_mutex_unlock(&batcher->mutex));
}

uint64_t batcher_get_epoch(batcher_t *batcher)
{
	check_pthread(pthread_mutex_lock(&batcher->mutex));
	assert(batcher->remaining > 0);
	uint64_t epoch = batcher->epoch;
	check_pthread(pthread_mutex_unlock(&batcher->mutex));
	return epoch;
}

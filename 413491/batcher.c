#include "batcher.h"
#include "sync.h"

#include <assert.h>
#include <stdint.h>

bool batcher_init(batcher_t *batcher, batcher_finalize_fn finalize_epoch,
		  void *context)
{
	if (pthread_mutex_init(&batcher->mutex, NULL) != 0) {
		return false;
	}
	if (pthread_cond_init(&batcher->changed, NULL) != 0) {
		check_pthread(pthread_mutex_destroy(&batcher->mutex));
		return false;
	}
#ifdef BATCHER_STATS
	if (!batcher_stats_init(&batcher->stats)) {
		check_pthread(pthread_cond_destroy(&batcher->changed));
		check_pthread(pthread_mutex_destroy(&batcher->mutex));
		return false;
	}
#endif
	batcher->epoch = 0;
	batcher->remaining = 0;
	batcher->waiting = 0;
	batcher->finalize_epoch = finalize_epoch;
	batcher->finalize_context = context;
	return true;
}

void batcher_destroy(batcher_t *batcher)
{
	assert(batcher->remaining == 0 && batcher->waiting == 0);
	check_pthread(pthread_cond_destroy(&batcher->changed));
	check_pthread(pthread_mutex_destroy(&batcher->mutex));
#ifdef BATCHER_STATS
	batcher_stats_destroy(&batcher->stats);
#endif
}

uint64_t batcher_enter(batcher_t *batcher)
{
#ifdef BATCHER_STATS
	uint64_t started = batcher_stats_now();
#endif
	check_pthread(pthread_mutex_lock(&batcher->mutex));
#ifdef BATCHER_STATS
	uint64_t acquired = batcher_stats_now();
	batcher_thread_stats_t *stats =
		batcher_stats_thread_locked(&batcher->stats);
	uint64_t cond_ns = 0;
	uint64_t cond_calls = 0;
	uint64_t wait_started = 0, wait_returned = 0;
	uint64_t before_open_ns = 0, after_open_ns = 0;
#endif
	if (batcher->remaining == 0) {
		batcher->remaining = 1;
#ifdef BATCHER_STATS
		batcher->stats.batch_started = batcher_stats_now();
		batcher->stats.batch_size = 1;
#endif
	} else {
		uint64_t epoch = batcher->epoch;
		++batcher->waiting;
		while (batcher->epoch == epoch) {
#ifdef BATCHER_STATS
			uint64_t before_wait = batcher_stats_now();
			if (cond_calls == 0) {
				wait_started = before_wait;
			}
#endif
			check_pthread(pthread_cond_wait(&batcher->changed,
							&batcher->mutex));
#ifdef BATCHER_STATS
			wait_returned = batcher_stats_now();
			cond_ns += wait_returned - before_wait;
			++cond_calls;
#endif
		}
		// The last leaver already counted us in the new batch.
#ifdef BATCHER_STATS
		// This batch cannot finish before we return and eventually leave it.
		uint64_t opened = batcher->stats.batch_started;
		assert(wait_started <= opened && opened <= wait_returned);
		before_open_ns = opened - wait_started;
		after_open_ns = wait_returned - opened;
#endif
	}

	uint64_t epoch = batcher->epoch;

	check_pthread(pthread_mutex_unlock(&batcher->mutex));
#ifdef BATCHER_STATS
	uint64_t admitted = batcher_stats_now();
	batcher_stats_record(stats, STATS_ADMISSION, admitted - started);
	batcher_stats_record(stats, STATS_ENTER_MUTEX, acquired - started);
	if (cond_calls) {
		batcher_stats_record(stats, STATS_COND_WAIT, cond_ns);
		batcher_stats_record(stats, STATS_WAIT_WINDOW,
				     wait_returned - wait_started);
		batcher_stats_record(stats, STATS_WAIT_BEFORE_OPEN,
				     before_open_ns);
		batcher_stats_record(stats, STATS_WAIT_AFTER_OPEN,
				     after_open_ns);
		if (stats) {
			stats->cond_calls += cond_calls;
		}
	}
#endif

	return epoch;
}

void batcher_leave(batcher_t *batcher)
{
#ifdef BATCHER_STATS
	batcher_thread_stats_t *stats = pthread_getspecific(batcher->stats.key);
	assert(!stats || stats->leave_lock_pending);
	uint64_t acquired = stats ? stats->leave_acquired_ns : batcher_stats_now();
	uint64_t active_ns = 0, finalize_ns = 0, broadcast_ns = 0,
		 batch_size = 0;
#endif
	assert(batcher->remaining > 0);
	if (--batcher->remaining == 0) {
#ifdef BATCHER_STATS
		batch_size = batcher->stats.batch_size;
		active_ns = acquired - batcher->stats.batch_started;
		uint64_t finalize_started = batcher_stats_now();
#endif
		if (batcher->finalize_epoch) {
			batcher->finalize_epoch(batcher->finalize_context);
		}
#ifdef BATCHER_STATS
		finalize_ns = batcher_stats_now() - finalize_started;
#endif
		++batcher->epoch;
		batcher->remaining = batcher->waiting;
		batcher->waiting = 0;
#ifdef BATCHER_STATS
		uint64_t before_broadcast = batcher_stats_now();
		batcher->stats.batch_started = before_broadcast;
		batcher->stats.batch_size = batcher->remaining;
#endif
		check_pthread(pthread_cond_broadcast(&batcher->changed));
#ifdef BATCHER_STATS
		broadcast_ns = batcher_stats_now() - before_broadcast;
#endif
	}

	check_pthread(pthread_mutex_unlock(&batcher->mutex));
#ifdef BATCHER_STATS
	if (stats) {
		stats->leave_lock_pending = false;
		batcher_stats_record(stats, STATS_LEAVE_MUTEX, stats->leave_wait_ns);
	}
	if (batch_size) {
		batcher_stats_record(stats, STATS_BATCH_ACTIVE, active_ns);
		batcher_stats_record(stats, STATS_BATCH_SIZE, batch_size);
		batcher_stats_record(stats, STATS_FINALIZE, finalize_ns);
		batcher_stats_record(stats, STATS_BROADCAST, broadcast_ns);
	}
#endif
}

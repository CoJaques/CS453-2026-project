#pragma once

#ifdef BATCHER_STATS

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

enum {
	STATS_ADMISSION,
	STATS_ENTER_MUTEX,
	STATS_COND_WAIT,
	STATS_WAIT_WINDOW,
	STATS_WAIT_BEFORE_OPEN,
	STATS_WAIT_AFTER_OPEN,
	STATS_LEAVE_MUTEX,
	STATS_BATCH_ACTIVE,
	STATS_FINALIZE,
	STATS_BROADCAST,
	STATS_BATCH_SIZE,
	STATS_METRIC_COUNT
};

typedef struct {
	uint64_t count;
	uint64_t sum;
	uint64_t maximum;
	uint64_t histogram[65]; // Bin k contains values <= 2^k; last bin saturates.
} batcher_metric_t;

typedef struct batcher_thread_stats_t {
	batcher_metric_t metrics[STATS_METRIC_COUNT];
	uint64_t cond_calls;
	uint64_t leave_wait_ns;
	uint64_t leave_acquired_ns;
	bool leave_lock_pending;
	struct batcher_thread_stats_t *next;
} batcher_thread_stats_t;

typedef struct {
	pthread_key_t key;
	batcher_thread_stats_t *threads; // Registered under the batcher mutex.
	uint64_t registration_failures;
	uint64_t batch_started; // Set under the mutex, before notifying the new batch.
	uint64_t batch_size;
} batcher_stats_t;

typedef struct {
	batcher_metric_t metrics[STATS_METRIC_COUNT];
	uint64_t threads;
	uint64_t cond_calls;
} batcher_stats_summary_t;

/** Initialize diagnostic state before any thread accesses the batcher.
 * @param stats Uninitialized diagnostic state
 * @return Whether the thread-local key was created successfully
**/
bool batcher_stats_init(batcher_stats_t *stats);

/** Get or register this thread's counters while the batcher mutex is held.
 * Records remain owned by the batcher even after their threads exit.
 * @param stats Diagnostic state for this batcher
 * @return Thread-owned record, or NULL if diagnostic allocation fails
**/
batcher_thread_stats_t *batcher_stats_thread_locked(batcher_stats_t *stats);

/** Read a monotonic timestamp for elapsed-time measurements.
 * @return Nanoseconds on CLOCK_MONOTONIC
**/
uint64_t batcher_stats_now(void);

/** Record one observation in thread-owned counters, outside the batcher mutex.
 * @param thread Record belonging to the caller, or NULL to skip the observation
 * @param metric Metric index
 * @param value Duration in nanoseconds, or participant count for STATS_BATCH_SIZE
**/
void batcher_stats_record(batcher_thread_stats_t *thread, unsigned int metric,
			  uint64_t value);

/** Aggregate counters after all concurrent batcher calls have finished.
 * @param stats Diagnostic state to read
 * @param summary Destination overwritten with aggregated counters
**/
void batcher_stats_collect(const batcher_stats_t *stats,
			   batcher_stats_summary_t *summary);

/** Print the summary to stderr and release records with no concurrent callers.
 * @param stats Initialized diagnostic state to destroy
**/
void batcher_stats_destroy(batcher_stats_t *stats);

#endif

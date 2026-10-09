#define _POSIX_C_SOURCE 200809L

#include "batcher_stats.h"

#ifdef BATCHER_STATS

#include "sync.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

bool batcher_stats_init(batcher_stats_t *stats)
{
	*stats = (batcher_stats_t){ 0 };
	return pthread_key_create(&stats->key, NULL) == 0;
}

batcher_thread_stats_t *batcher_stats_thread_locked(batcher_stats_t *stats)
{
	batcher_thread_stats_t *thread = pthread_getspecific(stats->key);
	if (!thread) {
		thread = calloc(1, sizeof(*thread));
		if (!thread) {
			++stats->registration_failures;
			return NULL;
		}
		if (pthread_setspecific(stats->key, thread) != 0) {
			free(thread);
			++stats->registration_failures;
			return NULL;
		}
		thread->next = stats->threads;
		stats->threads = thread;
	}
	return thread;
}

uint64_t batcher_stats_now(void)
{
	struct timespec now;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
		abort();
	}
	return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

void batcher_stats_record(batcher_thread_stats_t *thread, unsigned int metric,
			  uint64_t value)
{
	if (!thread) {
		return;
	}
	batcher_metric_t *entry = &thread->metrics[metric];
	++entry->count;
	entry->sum += value;
	if (value > entry->maximum) {
		entry->maximum = value;
	}
	unsigned int bin = 0;
	uint64_t remainder = value ? value - 1 : 0;
#ifdef __GNUC__
	if (remainder) {
		bin = 64 - __builtin_clzll(remainder);
	}
#else
	while (remainder) {
		++bin;
		remainder >>= 1;
	}
#endif
	++entry->histogram[bin];
}

void batcher_stats_collect(const batcher_stats_t *stats,
			   batcher_stats_summary_t *summary)
{
	*summary = (batcher_stats_summary_t){ 0 };
	for (const batcher_thread_stats_t *thread = stats->threads; thread;
	     thread = thread->next) {
		++summary->threads;
		summary->cond_calls += thread->cond_calls;
		for (unsigned int i = 0; i < STATS_METRIC_COUNT; ++i) {
			batcher_metric_t *target = &summary->metrics[i];
			const batcher_metric_t *source = &thread->metrics[i];
			target->count += source->count;
			target->sum += source->sum;
			if (source->maximum > target->maximum) {
				target->maximum = source->maximum;
			}
			for (unsigned int bin = 0; bin < 65; ++bin) {
				target->histogram[bin] += source->histogram[bin];
			}
		}
	}
}

/** Find a percentile's upper bound in a base-two histogram.
 * @param metric Aggregated observations
 * @param percent Percentile in the range 1 to 100
 * @return Upper bound capped by the observed maximum, or zero if empty
**/
static uint64_t percentile_bound(const batcher_metric_t *metric,
				 unsigned int percent)
{
	if (!metric->count) {
		return 0;
	}
	uint64_t rank = metric->count / 100 * percent +
		(metric->count % 100 * percent + 99) / 100;
	uint64_t cumulative = 0;
	for (unsigned int bin = 0; bin < 65; ++bin) {
		cumulative += metric->histogram[bin];
		if (cumulative >= rank) {
			uint64_t bound = bin == 64 ? UINT64_MAX : UINT64_C(1) << bin;
			return bound < metric->maximum ? bound : metric->maximum;
		}
	}
	return metric->maximum;
}

void batcher_stats_destroy(batcher_stats_t *stats)
{
	static const char *names[STATS_METRIC_COUNT] = {
		"admission", "enter_mutex", "cond_wait", "wait_window",
		"wait_before_open", "wait_after_open", "leave_mutex",
		"batch_active", "finalize", "broadcast",
		"batch_size"
	};
	batcher_stats_summary_t summary;
	batcher_stats_collect(stats, &summary);
	uint64_t entries = summary.metrics[STATS_ADMISSION].count;
	if (entries || stats->registration_failures) {
		flockfile(stderr);
		fprintf(stderr, "\n[batcher-stats %p] threads=%" PRIu64
			" admissions=%" PRIu64 " waited=%" PRIu64 " (%.2f%%)"
			" cond_calls=%" PRIu64 " registration_failures=%" PRIu64 "\n",
			(void *)stats, summary.threads, entries,
			summary.metrics[STATS_COND_WAIT].count,
			entries ? 100.0 * summary.metrics[STATS_COND_WAIT].count / entries : 0.0,
			summary.cond_calls, stats->registration_failures);
		fprintf(stderr, "Durations in us; batch_size in participants. "
			"Percentiles are histogram upper bounds.\n"
			"cond_wait includes mutex reacquisition and only waiting admissions.\n"
			"wait_window = wait_before_open + wait_after_open; opening is before broadcast.\n"
			"%-18s %12s %12s %12s %12s %12s %12s\n",
			"metric", "count", "mean", "p50<=", "p95<=", "p99<=", "max");
		for (unsigned int i = 0; i < STATS_METRIC_COUNT; ++i) {
			const batcher_metric_t *metric = &summary.metrics[i];
			double scale = i == STATS_BATCH_SIZE ? 1.0 : 1000.0;
			fprintf(stderr, "%-18s %12" PRIu64 " %12.3f %12.3f %12.3f %12.3f %12.3f\n",
				names[i], metric->count,
				metric->count ? (double)metric->sum / metric->count / scale : 0.0,
				percentile_bound(metric, 50) / scale,
				percentile_bound(metric, 95) / scale,
				percentile_bound(metric, 99) / scale,
				metric->maximum / scale);
		}
		uint64_t waiting_ns = summary.metrics[STATS_WAIT_WINDOW].sum;
		if (waiting_ns) {
			fprintf(stderr, "Waiting window split: before_open=%.2f%% after_open=%.2f%% "
				"(summed thread durations, not wall time).\n",
				100.0 * summary.metrics[STATS_WAIT_BEFORE_OPEN].sum / waiting_ns,
				100.0 * summary.metrics[STATS_WAIT_AFTER_OPEN].sum / waiting_ns);
		}
		funlockfile(stderr);
	}
	check_pthread(pthread_key_delete(stats->key));
	while (stats->threads) {
		batcher_thread_stats_t *thread = stats->threads;
		stats->threads = thread->next;
		free(thread);
	}
}

#endif

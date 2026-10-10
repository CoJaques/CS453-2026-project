#define _GNU_SOURCE

#include "batcher.h"

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

#ifndef BATCHER_SPIN_LIMIT
#define BATCHER_SPIN_LIMIT 128
#endif
#if BATCHER_SPIN_LIMIT < 0
#error "BATCHER_SPIN_LIMIT must be nonnegative"
#endif

bool batcher_init(batcher_t *batcher, batcher_finalize_fn finalize_epoch,
		  void *context)
{
	if (pthread_mutex_init(&batcher->mutex, NULL) != 0) {
		return false;
	}
	batcher->epoch = 0;
	atomic_init(&batcher->changed, 0);
	batcher->remaining = 0;
	batcher->waiting = 0;
	batcher->finalize_epoch = finalize_epoch;
	batcher->finalize_context = context;
	batcher->closed_ro = false;
	batcher->closed_rw = false;
	return true;
}

void batcher_destroy(batcher_t *batcher)
{
	assert(batcher->remaining == 0 && batcher->waiting == 0);
	check_pthread(pthread_mutex_destroy(&batcher->mutex));
}

/** Wait for publication without reacquiring the batcher mutex.
 * @param batcher Batcher with a reserved place for the caller in its next batch
 * @param notification Signal value captured during registration under the mutex
**/
static void batcher_wait(batcher_t *batcher, uint32_t notification)
{
	// Spin once, with a fixed budget; long waits still sleep in the kernel.
	unsigned int spins = BATCHER_SPIN_LIMIT;
	while (spins-- != 0) {
		if (atomic_load_explicit(&batcher->changed, memory_order_acquire) !=
		    notification) {
			return;
		}
#if defined(__x86_64__) || defined(__i386__)
		__builtin_ia32_pause();
#elif defined(__aarch64__)
		__asm__ __volatile__("yield" ::: "memory");
#else
		atomic_signal_fence(memory_order_seq_cst);
#endif
	}
	// Acquire makes the published snapshot visible. Futex checks the value
	// before sleeping, avoiding a lost wake between this load and the syscall.
	while (atomic_load_explicit(&batcher->changed, memory_order_acquire) ==
	       notification) {
		long result = syscall(SYS_futex, &batcher->changed,
				      FUTEX_WAIT_PRIVATE, notification, NULL,
				      NULL, 0);
		if (result < 0 && errno != EAGAIN && errno != EINTR) {
			abort();
		}
	}
}

/** Wake queued participants after updating the publication signal.
 * @param batcher Batcher whose mutex is held throughout the transition
**/
static void batcher_wake_all(batcher_t *batcher)
{
	if (syscall(SYS_futex, &batcher->changed, FUTEX_WAKE_PRIVATE, INT_MAX,
		    NULL, NULL, 0) < 0) {
		abort();
	}
}

uint64_t batcher_enter(batcher_t *batcher, bool is_ro)
{
	check_pthread(pthread_mutex_lock(&batcher->mutex));

	uint64_t epoch = batcher->epoch;
	bool queued = is_ro ? batcher->closed_ro : batcher->closed_rw;
	uint32_t notification =
		atomic_load_explicit(&batcher->changed, memory_order_relaxed);

	if (queued) {
		++batcher->waiting;
	} else {
		++batcher->remaining;
	}
	check_pthread(pthread_mutex_unlock(&batcher->mutex));

	if (queued) {
		// Already counted in the next batch, which cannot end until we leave.
		// Thus no transition can be missed, even when the 32-bit signal wraps.
		batcher_wait(batcher, notification);
		++epoch;
	}

	return epoch;
}

void batcher_leave_locked(batcher_t *batcher)
{
	assert(batcher->remaining > 0);

	if (--batcher->remaining == 0) {
		// No arrival can observe zero until publication completes: we hold mutex.
		if (batcher->finalize_epoch) {
			batcher->finalize_epoch(batcher->finalize_context);
		}

		size_t next_size = batcher->waiting;
		batcher->remaining = next_size;
		batcher->waiting = 0;
		++batcher->epoch;
		batcher->closed_ro = false;
		batcher->closed_rw = false;
		// Publish writes and the next participant count before admitting waiters.
		atomic_store_explicit(&batcher->changed,
				      (uint32_t)batcher->epoch,
				      memory_order_release);
		if (next_size) {
			batcher_wake_all(batcher);
		}
	}
	check_pthread(pthread_mutex_unlock(&batcher->mutex));
}

void batcher_leave(batcher_t *batcher)
{
	batcher_lock_for_leave(batcher);
	batcher_leave_locked(batcher);
}

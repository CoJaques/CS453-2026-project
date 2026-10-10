/**
 * @file   tm.c
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
**/

#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#ifdef __STDC_NO_ATOMICS__
#error Current C11 compiler does not support atomic operations
#endif

// Requested features
#include <pthread.h>
#include <assert.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>

// External headers

// Internal headers
#include <tm.h>

#include "types.h"
#include "macros.h"
#include "storage.h"
#include "sync.h"

/** Reset obsolete word access metadata while its mutex is held by the caller.
 * @param status Word control protected by the acquired mutex
 * @param epoch Epoch of the transaction accessing the word
**/
static inline void word_prepare_epoch(word_status_t *status, uint64_t epoch)
{
	if (status->epoch != epoch) {
		status->epoch = epoch;
		status->written = false;
		status->owner = 0;
	}
}

/** Release a context and its unpublished allocations without leaving the batch.
 * @param transaction Heap-allocated context to destroy
**/
static void transaction_discard(transaction_t *transaction)
{
	// Tentative writes are never published; word access marks expire by epoch.
	if (transaction->written_words != transaction->inline_words) {
		free(transaction->written_words);
	}
	while (transaction->segment_to_free) {
		segment_ref_t *ref = transaction->segment_to_free;
		transaction->segment_to_free = ref->next;
		free(ref);
	}
	while (transaction->segment_allocated) {
		segment_ref_t *ref = transaction->segment_allocated;
		transaction->segment_allocated = ref->next;
		segment_destroy(ref->segment);
		free(ref);
	}
	free(transaction);
}

/** Abandon a transaction and leave its batch after releasing private resources.
 * @param region Region in whose batch the transaction participates
 * @param transaction Heap-allocated context to destroy
**/
static void transaction_abort(region_t *region, transaction_t *transaction)
{
	assert(!transaction->is_ro);
	batcher_lock_for_leave(region->batcher);
	// Close before freeing the context: its address remains a word owner this epoch.
	region->batcher->closed_rw = true;
	transaction_discard(transaction);
	batcher_leave_locked(region->batcher);
}

/** Publish committed effects before the next batch starts running.
 * Called with the batcher mutex held and no active transactions in this region.
 * Must not lock that mutex again or call batcher_enter/leave.
 * @param context Region whose completed epoch is being finalized
**/
static void region_finalize_epoch(void *context)
{
	assert(context != NULL);

	region_t *region = (region_t *)context;
	transaction_t *committed = region->committed;
	region->committed = NULL;

	// Copy committed words into the snapshot before any segment can be destroyed.
	for (transaction_t *tx = committed; tx; tx = tx->next_committed) {
		assert(tx->epoch == region->batcher->epoch);
		for (size_t i = 0; i < tx->written_count; ++i) {
			word_ref_t word = tx->written_words[i];
			size_t offset = word.index << region->align_shift;
			memcpy((unsigned char *)word.segment
					       ->data[DATA_COMMITTED] +
				       offset,
			       (unsigned char *)word.segment->data[DATA_PENDING] +
				       offset,
			       region->align);
		}
	}

	// Transfer segment ownership, preserving the initial segment at the head.
	for (transaction_t *tx = committed; tx; tx = tx->next_committed) {
		while (tx->segment_allocated) {
			segment_ref_t *ref = tx->segment_allocated;
			tx->segment_allocated = ref->next;
			ref->segment->next = region->head->next;
			region->head->next = ref->segment;
			free(ref);
		}
	}

	// Deduplicate before destroying any segment; reuse the existing request nodes.
	segment_ref_t *unique_frees = NULL;
	for (transaction_t *tx = committed; tx; tx = tx->next_committed) {
		while (tx->segment_to_free) {
			segment_ref_t *ref = tx->segment_to_free;
			tx->segment_to_free = ref->next;
			segment_ref_t *existing = unique_frees;
			while (existing && existing->segment != ref->segment) {
				existing = existing->next;
			}
			if (existing) {
				free(ref);
			} else {
				ref->next = unique_frees;
				unique_frees = ref;
			}
		}
	}

	// All allocations have been published, including those freed in the same batch.
	while (unique_frees) {
		segment_ref_t *ref = unique_frees;
		unique_frees = ref->next;
		segment_t **link = &region->head->next;
		while (*link && *link != ref->segment) {
			link = &(*link)->next;
		}
		assert(*link == ref->segment);
		*link = ref->segment->next;
		segment_destroy(ref->segment);
		free(ref);
	}

	// Access metadata expires lazily; only the completed contexts need cleanup here.
	while (committed) {
		transaction_t *next = committed->next_committed;
		if (committed->written_words != committed->inline_words) {
			free(committed->written_words);
		}
		free(committed);
		committed = next;
	}
}

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align)
{
	if (size == 0 || align == 0 || (align & (align - 1)) != 0 ||
	    (size & (align - 1)) != 0) {
		return invalid_shared;
	}

	region_t *region = malloc(sizeof(region_t));
	if (unlikely(!region)) {
		return invalid_shared;
	}

	region->size = size;
	region->align = align;
	region->align_shift = 0;
	for (size_t word_size = align; word_size > 1; word_size >>= 1) {
		++region->align_shift;
	}
	region->committed = NULL;

	for (size_t i = 0; i < WORD_LOCK_COUNT; ++i) {
		if (pthread_mutex_init(&region->word_locks[i], NULL) != 0) {
			word_locks_destroy(region, i);
			free(region);
			return invalid_shared;
		}
	}

	batcher_t *batcher =
		aligned_alloc(_Alignof(batcher_t), sizeof(batcher_t));
	if (unlikely(!batcher)) {
		word_locks_destroy(region, WORD_LOCK_COUNT);
		free(region);
		return invalid_shared;
	}
	if (unlikely(!batcher_init(batcher, region_finalize_epoch, region))) {
		free(batcher);
		word_locks_destroy(region, WORD_LOCK_COUNT);
		free(region);
		return invalid_shared;
	}

	region->batcher = batcher;

	segment_t *initial_segment =
		segment_create(size, align, region->align_shift);

	if (unlikely(!initial_segment)) {
		batcher_destroy(batcher);
		free(batcher);
		word_locks_destroy(region, WORD_LOCK_COUNT);
		free(region);
		return invalid_shared;
	}

	region->head = initial_segment;

	return (shared_t)region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t unused(shared))
{
	region_t *region = (region_t *)shared;

	if (unlikely(!region)) {
		return;
	}

	// Also release contexts whose effects have not yet been published.
	while (region->committed) {
		transaction_t *transaction = region->committed;
		region->committed = transaction->next_committed;
		transaction_discard(transaction);
	}
	segment_t *current = region->head;

	while (current) {
		segment_t *next = current->next;
		segment_destroy(current);
		current = next;
	}

	batcher_destroy(region->batcher);
	free(region->batcher);
	word_locks_destroy(region, WORD_LOCK_COUNT);
	free(region);

	return;
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void *tm_start(shared_t shared)
{
	region_t *region = (region_t *)shared;
	// Public addresses always point into the committed snapshot.
	return region->head->data[DATA_COMMITTED];
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared)
{
	region_t *region = (region_t *)shared;
	return region->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared)
{
	region_t *region = (region_t *)shared;
	return region->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro)
{
	assert(shared != invalid_shared);
	region_t *region = (region_t *)shared;

	transaction_t *transaction = calloc(1, sizeof(transaction_t));
	if (unlikely(!transaction)) {
		return invalid_tx;
	}

	transaction->id = (tx_t)transaction;
	transaction->is_ro = is_ro;
	transaction->written_words = transaction->inline_words;
	transaction->written_capacity =
		sizeof(transaction->inline_words) / sizeof(word_ref_t);

	transaction->epoch = batcher_enter(region->batcher, is_ro);

	return (tx_t)transaction;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx)
{
	assert(shared != invalid_shared && tx != 0 && tx != invalid_tx);

	region_t *region = (region_t *)shared;
	transaction_t *transaction = (transaction_t *)tx;

	if (transaction->is_ro) {
		free(transaction);
		batcher_leave(region->batcher);
		return true;
	}

	// Retain the context and its logs until publication at the epoch boundary.
	// Registration and departure share one acquisition of the mutex.
	batcher_lock_for_leave(region->batcher);
	region->batcher->closed_ro = true;
	transaction->next_committed = region->committed;
	region->committed = transaction;

	batcher_leave_locked(region->batcher);
	return true;
}

/** Read shared words in a read-write transaction after API argument checks.
 * Kept out of line so read-only accesses do not inherit the RW stack setup.
 * @param region Region owning the shared words and their mutexes
 * @param transaction Active read-write transaction accessing the words
 * @param source Valid shared source address aligned to the region's word size
 * @param size Positive byte count, a multiple of the region's word size
 * @param target Private destination buffer, possibly unaligned
 * @return true on success, or false after aborting the transaction on conflict
**/
#ifdef __GNUC__
__attribute__((noinline))
#endif
static bool transaction_read_rw(region_t *region, transaction_t *transaction,
				void const *source, size_t size, void *target)
{
	segment_t *segment =
		transaction_find_segment(region, transaction, source);
	assert(segment != NULL);
	size_t offset =
		(uintptr_t)source - (uintptr_t)segment->data[DATA_COMMITTED];
	assert(size <= segment->size - offset);

	for (size_t done = 0; done < size; done += region->align) {
		size_t word_offset = offset + done;
		size_t index = word_offset >> region->align_shift;
		word_status_t *status = &segment->status[index];
		pthread_mutex_t *mutex = word_mutex_for(region, segment, index);
		check_pthread(pthread_mutex_lock(mutex));
		word_prepare_epoch(status, transaction->epoch);

		if (status->written && status->owner != transaction->id) {
			// Abort may destroy private segments, so release the word first.
			check_pthread(pthread_mutex_unlock(mutex));
			transaction_abort(region, transaction);
			return false;
		} else if (status->owner == 0) {
			status->owner = transaction->id;
		} else if (status->owner != transaction->id) {
			status->owner = invalid_tx;
		}

		unsigned int buffer = status->written ? DATA_PENDING :
							DATA_COMMITTED;
		memcpy((unsigned char *)target + done,
		       (unsigned char *)segment->data[buffer] + word_offset,
		       region->align);
		check_pthread(pthread_mutex_unlock(mutex));
	}
	return true;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared, tx_t tx, void const *source, size_t size,
	     void *target)
{
	assert(shared != invalid_shared && source != NULL && target != NULL);
	assert(tx != 0 && tx != invalid_tx);

	transaction_t *transaction = (transaction_t *)tx;
	region_t *region = (region_t *)shared;
	assert(size > 0 && (size & (region->align - 1)) == 0);
	assert(((uintptr_t)source & (region->align - 1)) == 0);
	if (transaction->is_ro) {
		// Public addresses refer to DATA_COMMITTED, stable until this batch ends.
		// memcpy also accepts an unaligned private destination.
		if (size == sizeof(uint64_t)) {
			memcpy(target, source, sizeof(uint64_t));
		} else {
			memcpy(target, source, size);
		}
		return true;
	}

	return transaction_read_rw(region, transaction, source, size, target);
}

/** Record a word on its first write by the transaction.
 * @param transaction Context owning the growable write log
 * @param segment Segment containing the word
 * @param index Word index within the segment
 * @return Whether the entry was added successfully
**/
static bool transaction_record_write(transaction_t *transaction,
				     segment_t *segment, size_t index)
{
	if (transaction->written_count == transaction->written_capacity) {
		size_t maximum = SIZE_MAX / sizeof(word_ref_t);
		size_t capacity = transaction->written_capacity;
		if (capacity >= maximum) {
			return false;
		}
		capacity = capacity > maximum / 2 ? maximum : capacity * 2;
		word_ref_t *words;
		if (transaction->written_words == transaction->inline_words) {
			words = malloc(capacity * sizeof(word_ref_t));
			if (words) {
				memcpy(words, transaction->inline_words,
				       transaction->written_count * sizeof(word_ref_t));
			}
		} else {
			words = realloc(transaction->written_words,
					capacity * sizeof(word_ref_t));
		}
		if (!words) {
			return false;
		}
		transaction->written_words = words;
		transaction->written_capacity = capacity;
	}
	transaction->written_words[transaction->written_count++] =
		(word_ref_t){ .segment = segment, .index = index };
	return true;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared, tx_t tx, void const *source, size_t size,
	      void *target)
{
	assert(shared != invalid_shared && source != NULL && target != NULL);
	assert(tx != 0 && tx != invalid_tx);

	transaction_t *transaction = (transaction_t *)tx;
	region_t *region = (region_t *)shared;
	assert(size > 0 && (size & (region->align - 1)) == 0);
	assert(((uintptr_t)target & (region->align - 1)) == 0);
	// The private source may be unaligned; memcpy handles byte buffers.
	assert(!transaction->is_ro);
	segment_t *segment =
		transaction_find_segment(region, transaction, target);
	assert(segment != NULL);
	size_t offset =
		(uintptr_t)target - (uintptr_t)segment->data[DATA_COMMITTED];
	assert(size <= segment->size - offset);

	for (size_t done = 0; done < size; done += region->align) {
		size_t word_offset = offset + done;
		size_t index = word_offset >> region->align_shift;
		word_status_t *status = &segment->status[index];
		pthread_mutex_t *mutex = word_mutex_for(region, segment, index);
		check_pthread(pthread_mutex_lock(mutex));
		word_prepare_epoch(status, transaction->epoch);

		if (status->owner != 0 && status->owner != transaction->id) {
			check_pthread(pthread_mutex_unlock(mutex));
			transaction_abort(shared, transaction);
			return false;
		}

		if (!status->written) {
			bool success = transaction_record_write(transaction,
								segment, index);
			if (!success) {
				check_pthread(pthread_mutex_unlock(mutex));
				transaction_abort(shared, transaction);
				return false;
			}
		}

		memcpy((unsigned char *)segment->data[DATA_PENDING] +
			       word_offset,
		       (unsigned char const *)source + done, region->align);
		status->owner = transaction->id;
		status->written = true;

		check_pthread(pthread_mutex_unlock(mutex));
	}

	return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t shared, tx_t tx, size_t size, void **target)
{
	// Invalid arguments violate the API contract; they are not transaction aborts.
	assert(shared != invalid_shared && target != NULL);
	assert(tx != 0 && tx != invalid_tx);
	region_t *region = (region_t *)shared;
	assert(size > 0 && (size & (region->align - 1)) == 0);

	transaction_t *transaction = (transaction_t *)tx;
	assert(!transaction->is_ro);

	segment_t *segment =
		segment_create(size, region->align, region->align_shift);

	if (unlikely(!segment)) {
		return nomem_alloc;
	}

	segment_ref_t *segment_ref = malloc(sizeof(segment_ref_t));

	if (unlikely(!segment_ref)) {
		segment_destroy(segment);
		return nomem_alloc;
	}

	segment_ref->segment = segment;

	segment_ref->next = transaction->segment_allocated;
	transaction->segment_allocated = segment_ref;

	*target = segment->data[DATA_COMMITTED];
	return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void *target)
{
	assert(shared != invalid_shared && target != NULL);
	assert(tx != 0 && tx != invalid_tx);
	region_t *region = (region_t *)shared;
	transaction_t *transaction = (transaction_t *)tx;
	assert(!transaction->is_ro);
	assert(target != region->head->data[DATA_COMMITTED]);

	// One request per segment prevents duplicate destruction in this transaction.
	for (segment_ref_t *ref = transaction->segment_to_free; ref;
	     ref = ref->next) {
		if (ref->segment->data[DATA_COMMITTED] == target) {
			return true;
		}
	}

	segment_t *segment =
		transaction_find_segment(region, transaction, target);
	// Only the public base address of a live, accessible segment is valid.
	assert(segment != NULL && segment->data[DATA_COMMITTED] == target);

	segment_ref_t *ref = malloc(sizeof(segment_ref_t));
	if (unlikely(!ref)) {
		// Unlike tm_alloc, this API has no non-aborting out-of-memory result.
		transaction_abort(region, transaction);
		return false;
	}

	ref->segment = segment;
	ref->next = transaction->segment_to_free;
	transaction->segment_to_free = ref;
	return true;
}

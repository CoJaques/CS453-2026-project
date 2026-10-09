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

/** Find the shared mutex protecting a word's metadata and read-write accesses.
 * @param region Region owning the lock table
 * @param segment Segment containing the word
 * @param index Word index within the segment
 * @return Stable mutex for this word's read-write accesses
**/
static pthread_mutex_t *word_mutex_for(region_t *region, segment_t *segment,
				     size_t index)
{
	uintptr_t key = ((uintptr_t)segment->data[0] >> region->align_shift) + index;
	key ^= key >> 10;
	key ^= key >> 20;
	return &region->word_locks[key % WORD_LOCK_COUNT];
}

/** Destroy the initialized prefix of a region's word lock table.
 * @param region Region owning the mutexes
 * @param count Number of successfully initialized mutexes
**/
static void word_locks_destroy(region_t *region, size_t count)
{
	for (size_t i = 0; i < count; ++i) {
		if (pthread_mutex_destroy(&region->word_locks[i]) != 0) {
			abort();
		}
	}
}

/** Destroy a fully initialized segment with no concurrent users.
 * @param segment Segment whose buffers and controls are released
**/
static void segment_destroy(segment_t *segment)
{
	// The controls and both copies are interior pointers into this allocation.
	free(segment);
}

/** Create a zero-initialized dual-versioned segment in one allocation.
 * @param size Positive segment size, a multiple of align
 * @param align Positive power-of-two word size
 * @param align_shift Precomputed log2(align)
 * @return Initialized segment, or NULL after cleanup on failure
**/
static segment_t *segment_create(size_t size, size_t align,
				 unsigned int align_shift)
{
	// calloc aligns the base for both structures; round the controls' offset too.
	size_t status_align = _Alignof(word_status_t);
	size_t status_offset = sizeof(segment_t) +
		(status_align - sizeof(segment_t) % status_align) % status_align;
	size_t word_count = size >> align_shift;
	if (word_count > (SIZE_MAX - status_offset) / sizeof(word_status_t)) {
		return NULL;
	}
	size_t data_offset = status_offset + word_count * sizeof(word_status_t);
	// Reserve enough padding to align the actual data address, not just its offset.
	if (align - 1 > SIZE_MAX - data_offset) {
		return NULL;
	}
	size_t overhead = data_offset + align - 1;
	if (size > (SIZE_MAX - overhead) / 2) {
		return NULL;
	}

	segment_t *segment = calloc(1, overhead + 2 * size);
	if (unlikely(!segment)) {
		return NULL;
	}
	unsigned char *base = (unsigned char *)segment;
	unsigned char *data = base + data_offset;
	size_t remainder = (uintptr_t)data & (align - 1);
	if (remainder != 0) {
		data += align - remainder;
	}
	segment->size = size;
	segment->next = NULL;
	segment->status = (word_status_t *)(base + status_offset);
	segment->data[0] = data;
	segment->data[1] = data + size; // size is a multiple of align.
	// Zeroed controls mean unwritten, epoch 0 and ACCESS_NONE.
	// owner is ignored in ACCESS_NONE, so no per-word initialization is needed.
	return segment;
}

/** Release a context and its unpublished allocations without leaving the batch.
 * @param transaction Heap-allocated context to destroy
**/
static void transaction_discard(transaction_t *transaction)
{
	// Tentative writes are never published; word access marks expire by epoch.
	free(transaction->written_words);
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
	transaction_discard(transaction);
	batcher_leave(region->batcher);
}

/** Publish committed effects before the next batch starts running.
 * Called with the batcher mutex held and no active transactions in this region.
 * Must not lock that mutex again or call batcher_enter/leave/get_epoch.
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
			memcpy((unsigned char *)word.segment->data[0] + offset,
			       (unsigned char *)word.segment->data[1] + offset,
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
		free(committed->written_words);
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

	batcher_t *batcher = malloc(sizeof(batcher_t));
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

	segment_t *initial_segment = segment_create(size, align, region->align_shift);

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
	return region->head->data[0];
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

	batcher_enter(region->batcher);
	transaction->epoch = batcher_get_epoch(region->batcher);

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
	if (pthread_mutex_lock(&region->batcher->mutex) != 0) {
		abort();
	}
	transaction->next_committed = region->committed;
	region->committed = transaction;
	if (pthread_mutex_unlock(&region->batcher->mutex) != 0) {
		abort();
	}

	batcher_leave(region->batcher);
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
		// Valid public addresses refer to data[0], stable until this batch ends.
		// memcpy also accepts an unaligned private destination.
		memcpy(target, source, size);
		return true;
	}

	uintptr_t address = (uintptr_t)source;
	segment_t *segment = NULL;

	for (segment_ref_t *ref = transaction->segment_allocated; ref;
	     ref = ref->next) {
		uintptr_t base = (uintptr_t)ref->segment->data[0];
		if (address >= base && address - base < ref->segment->size) {
			segment = ref->segment;
			break;
		}
	}
	if (!segment) {
		// The published list stays stable throughout the current epoch.
		for (segment = region->head; segment; segment = segment->next) {
			uintptr_t base = (uintptr_t)segment->data[0];
			if (address >= base && address - base < segment->size) {
				break;
			}
		}
	}
	assert(segment != NULL);
	size_t offset = address - (uintptr_t)segment->data[0];
	assert(size <= segment->size - offset);

	for (size_t done = 0; done < size; done += region->align) {
		size_t word_offset = offset + done;
		size_t index = word_offset >> region->align_shift;
		word_status_t *status = &segment->status[index];
		pthread_mutex_t *mutex = word_mutex_for(region, segment, index);
		if (pthread_mutex_lock(mutex) != 0) {
			abort();
		}
		if (status->epoch != transaction->epoch) {
			// The published copy persists; only access metadata expires.
			status->epoch = transaction->epoch;
			status->written = false;
			status->access_state = ACCESS_NONE;
			status->owner = invalid_tx;
		}

		unsigned int copy = 0;
		if (status->written) {
			if (status->access_state != ACCESS_ONE ||
			    status->owner != transaction->id) {
				// Abort may destroy private segments, so release the word first.
				if (pthread_mutex_unlock(mutex) != 0) {
					abort();
				}
				transaction_abort(region, transaction);
				return false;
			}
		} else if (status->access_state == ACCESS_NONE) {
			status->access_state = ACCESS_ONE;
			status->owner = transaction->id;
		} else if (status->access_state == ACCESS_ONE &&
			   status->owner != transaction->id) {
			status->access_state = ACCESS_MANY;
			status->owner = invalid_tx;
		}

		memcpy((unsigned char *)target + done,
		       (unsigned char *)segment->data[1] + word_offset,
		       region->align);
		if (pthread_mutex_unlock(mutex) != 0) {
			abort();
		}
	}
	return true;
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
		capacity = capacity == 0	  ? 8 :
			   capacity > maximum / 2 ? maximum :
						    capacity * 2;
		word_ref_t *words = realloc(transaction->written_words,
					    capacity * sizeof(word_ref_t));
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
	uintptr_t address = (uintptr_t)target;
	segment_t *segment = NULL;
	for (segment_ref_t *ref = transaction->segment_allocated; ref;
	     ref = ref->next) {
		uintptr_t base = (uintptr_t)ref->segment->data[0];
		if (address >= base && address - base < ref->segment->size) {
			segment = ref->segment;
			break;
		}
	}
	if (!segment) {
		// The published list stays stable throughout the current epoch.
		for (segment = region->head; segment; segment = segment->next) {
			uintptr_t base = (uintptr_t)segment->data[0];
			if (address >= base && address - base < segment->size) {
				break;
			}
		}
	}
	assert(segment != NULL);
	size_t offset = address - (uintptr_t)segment->data[0];
	assert(size <= segment->size - offset);

	for (size_t done = 0; done < size; done += region->align) {
		size_t word_offset = offset + done;
		size_t index = word_offset >> region->align_shift;
		word_status_t *status = &segment->status[index];
		pthread_mutex_t *mutex = word_mutex_for(region, segment, index);
		if (pthread_mutex_lock(mutex) != 0) {
			abort();
		}
		if (status->epoch != transaction->epoch) {
			// The published copy persists; only access metadata expires.
			status->epoch = transaction->epoch;
			status->written = false;
			status->access_state = ACCESS_NONE;
			status->owner = invalid_tx;
		}

		bool authorized = false;
		if (status->written) {
			authorized = status->access_state == ACCESS_ONE &&
				     status->owner == transaction->id;
		} else {
			authorized = status->access_state == ACCESS_NONE ||
				     (status->access_state == ACCESS_ONE &&
				      status->owner == transaction->id);
		}

		if (!authorized) {
			if (pthread_mutex_unlock(mutex) != 0) {
				abort();
			}
			transaction_abort(shared, transaction);
			return false;
		}

		if (!status->written) {
			bool success = transaction_record_write(transaction,
								segment, index);
			if (!success) {
				if (pthread_mutex_unlock(mutex) != 0) {
					abort();
				}
				transaction_abort(shared, transaction);
				return false;
			}
		}

		memcpy((unsigned char *)segment->data[1] + word_offset,
		       (unsigned char const *)source + done, region->align);
		status->access_state = ACCESS_ONE;
		status->owner = transaction->id;
		status->written = true;

		if (pthread_mutex_unlock(mutex) != 0) {
			abort();
		}
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

	segment_t *segment = segment_create(size, region->align, region->align_shift);

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

	*target = segment->data[0];
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
	assert(target != region->head->data[0]);

	// One request per segment prevents duplicate destruction in this transaction.
	for (segment_ref_t *ref = transaction->segment_to_free; ref;
	     ref = ref->next) {
		if (ref->segment->data[0] == target) {
			return true;
		}
	}

	segment_t *segment = NULL;
	for (segment_ref_t *ref = transaction->segment_allocated; ref;
	     ref = ref->next) {
		if (ref->segment->data[0] == target) {
			segment = ref->segment;
			break;
		}
	}

	if (!segment) {
		// Publish allocations and unlink freed segments only between epochs.
		// This keeps the region's segment list stable while transactions use it.
		for (segment = region->head->next; segment;
		     segment = segment->next) {
			if (segment->data[0] == target) {
				break;
			}
		}
	}
	// Only the public base address of a live, accessible segment is valid.
	assert(segment != NULL);

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

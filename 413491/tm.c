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
#include <assert.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>

// External headers

// Internal headers
#include <tm.h>

#include "types.h"
#include "macros.h"

/** Destroy a fully initialized segment with no concurrent users.
 * @param segment Segment whose buffers and controls are released
 * @param align Size of a word in the associated region
**/
static void segment_destroy(segment_t *segment, size_t align)
{
	for (size_t i = 0; i < segment->size / align; ++i) {
		if (pthread_mutex_destroy(&segment->status[i].mutex) != 0) {
			abort();
		}
	}
	free(segment->status);
	free(segment->data[1]);
	free(segment->data[0]);
	free(segment);
}

/** Create a zero-initialized dual-versioned segment.
 * @param size Positive segment size, a multiple of align
 * @param align Positive power-of-two word size
 * @return Initialized segment, or NULL after cleanup on failure
**/
static segment_t *segment_create(size_t size, size_t align)
{
	size_t word_count = size / align;
	if (word_count > SIZE_MAX / sizeof(word_status_t)) {
		return NULL;
	}

	segment_t *segment = calloc(1, sizeof(segment_t));
	if (unlikely(!segment)) {
		return NULL;
	}
	segment->size = size;
	segment->next = NULL;
	size_t initialized_words = 0;
	size_t allocation_align = align < sizeof(void *) ? sizeof(void *) :
							   align;

	for (size_t copy = 0; copy < 2; ++copy) {
		if (posix_memalign(&segment->data[copy], allocation_align,
				   size) != 0) {
			goto fail;
		}
		memset(segment->data[copy], 0, size);
	}

	segment->status = calloc(word_count, sizeof(word_status_t));
	if (unlikely(!segment->status)) {
		goto fail;
	}
	for (size_t i = 0; i < word_count; ++i) {
		word_status_t *status = &segment->status[i];
		status->read_copy = 0;
		status->written = false;
		status->epoch = 0;
		status->access_state = ACCESS_NONE;
		status->owner = invalid_tx;
		if (pthread_mutex_init(&status->mutex, NULL) != 0) {
			goto fail;
		}
		++initialized_words;
	}
	return segment;

fail:
	// Only mutexes whose initialization succeeded may be destroyed.
	for (size_t i = 0; i < initialized_words; ++i) {
		if (pthread_mutex_destroy(&segment->status[i].mutex) != 0) {
			abort();
		}
	}
	free(segment->status);
	free(segment->data[1]);
	free(segment->data[0]);
	free(segment);
	return NULL;
}

/** Abandon a transaction and leave its batch after releasing private resources.
 * @param region Region in whose batch the transaction participates
 * @param transaction Heap-allocated context to destroy
**/
static void transaction_abort(region_t *region, transaction_t *transaction)
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
		segment_destroy(ref->segment, region->align);
		free(ref);
	}
	free(transaction);
	batcher_leave(region->batcher);
}

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align)
{
	if (size == 0 || align == 0 || (size % align) != 0 ||
	    (align & (align - 1)) != 0) {
		return invalid_shared;
	}

	region_t *region = malloc(sizeof(region_t));
	if (unlikely(!region)) {
		return invalid_shared;
	}

	region->size = size;
	region->align = align;

	batcher_t *batcher = malloc(sizeof(batcher_t));
	if (unlikely(!batcher)) {
		free(region);
		return invalid_shared;
	}
	if (unlikely(!batcher_init(batcher))) {
		free(batcher);
		free(region);
		return invalid_shared;
	}

	region->batcher = batcher;

	segment_t *initial_segment = segment_create(size, align);

	if (unlikely(!initial_segment)) {
		batcher_destroy(batcher);
		free(batcher);
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

	segment_t *current = region->head;

	while (current) {
		segment_t *next = current->next;
		segment_destroy(current, region->align);
		current = next;
	}

	batcher_destroy(region->batcher);
	free(region->batcher);
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
	// The public address is stable even when a word's readable copy changes.
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
bool tm_end(shared_t unused(shared), tx_t unused(tx))
{
	// TODO: tm_end(shared_t, tx_t)
	return false;
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
	assert(size > 0 && size % tm_align(shared) == 0);

	transaction_t *transaction = (transaction_t *)tx;
	region_t *region = (region_t *)shared;
	assert((uintptr_t)source % region->align == 0);
	assert((uintptr_t)target % region->align == 0);
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

	if (transaction->is_ro) {
		// Readable copies and their selectors remain stable within this epoch.
		for (size_t done = 0; done < size; done += region->align) {
			size_t word_offset = offset + done;
			size_t index = word_offset / region->align;
			uint8_t copy = segment->status[index].read_copy;
			assert(copy < 2);
			memcpy((unsigned char *)target + done,
			       (unsigned char *)segment->data[copy] +
				       word_offset,
			       region->align);
		}
		return true;
	}

	for (size_t done = 0; done < size; done += region->align) {
		size_t word_offset = offset + done;
		size_t index = word_offset / region->align;
		word_status_t *status = &segment->status[index];
		if (pthread_mutex_lock(&status->mutex) != 0) {
			abort();
		}
		if (status->epoch != transaction->epoch) {
			// The published copy persists; only access metadata expires.
			status->epoch = transaction->epoch;
			status->written = false;
			status->access_state = ACCESS_NONE;
			status->owner = invalid_tx;
		}

		uint8_t copy = status->read_copy;
		assert(copy < 2);
		if (status->written) {
			if (status->access_state != ACCESS_ONE ||
			    status->owner != transaction->id) {
				// Abort may destroy private segments, so release the word first.
				if (pthread_mutex_unlock(&status->mutex) != 0) {
					abort();
				}
				transaction_abort(region, transaction);
				return false;
			}
			copy = 1 - copy;
		} else if (status->access_state == ACCESS_NONE) {
			status->access_state = ACCESS_ONE;
			status->owner = transaction->id;
		} else if (status->access_state == ACCESS_ONE &&
			   status->owner != transaction->id) {
			status->access_state = ACCESS_MANY;
			status->owner = invalid_tx;
		}

		memcpy((unsigned char *)target + done,
		       (unsigned char *)segment->data[copy] + word_offset,
		       region->align);
		if (pthread_mutex_unlock(&status->mutex) != 0) {
			abort();
		}
	}
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
	assert(size > 0 && size % tm_align(shared) == 0);

	transaction_t *transaction = (transaction_t *)tx;
	region_t *region = (region_t *)shared;
	assert((uintptr_t)source % region->align == 0);
	assert((uintptr_t)target % region->align == 0);
	assert(!transaction->is_ro);
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

	return false;
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
	assert(size > 0 && size % region->align == 0);

	transaction_t *transaction = (transaction_t *)tx;
	assert(!transaction->is_ro);

	segment_t *segment = segment_create(size, region->align);

	if (unlikely(!segment)) {
		return nomem_alloc;
	}

	segment_ref_t *segment_ref = malloc(sizeof(segment_ref_t));

	if (unlikely(!segment_ref)) {
		segment_destroy(segment, region->align);
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

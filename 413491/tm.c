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
#include <stdlib.h>
#include <stddef.h>
#include <string.h>

// External headers

// Internal headers
#include <tm.h>

#include "types.h"
#include "macros.h"

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

	segment_t *initial_segment = calloc(1, sizeof(segment_t));

	if (unlikely(!initial_segment)) {
		batcher_destroy(batcher);
		free(batcher);
		free(region);
		return invalid_shared;
	}

	region->head = initial_segment;

	if (unlikely(posix_memalign(&initial_segment->data, align, size) !=
		     0)) {
		free(initial_segment);
		batcher_destroy(batcher);
		free(batcher);
		free(region);
		return invalid_shared;
	}

	memset(initial_segment->data, 0, size);

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
		free(current->data);
		free(current);
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
	return region->head->data;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared)
{
	region_t *region = (region_t *)shared;
	return region->size;
	return 0;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared)
{
	region_t *region = (region_t *)shared;
	return region->align;
	return 0;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t unused(shared), bool unused(is_ro))
{
	// TODO: tm_begin(shared_t)
	return invalid_tx;
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
bool tm_read(shared_t unused(shared), tx_t unused(tx),
	     void const *unused(source), size_t unused(size),
	     void *unused(target))
{
	// TODO: tm_read(shared_t, tx_t, void const*, size_t, void*)
	return false;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t unused(shared), tx_t unused(tx),
	      void const *unused(source), size_t unused(size),
	      void *unused(target))
{
	// TODO: tm_write(shared_t, tx_t, void const*, size_t, void*)
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
	region_t *region = (region_t *)shared;

	return abort_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void *unused(target))
{
	// TODO: tm_free(shared_t, tx_t, void*)
	return false;
}

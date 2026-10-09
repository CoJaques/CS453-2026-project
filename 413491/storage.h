#pragma once

#include "types.h"

/** Find the shared mutex protecting a word's metadata and read-write accesses.
 * @param region Region owning the lock table
 * @param segment Segment containing the word
 * @param index Word index within the segment
 * @return Stable mutex for this word's read-write accesses
**/
static inline pthread_mutex_t *word_mutex_for(region_t *region, segment_t *segment,
					    size_t index)
{
	uintptr_t key = ((uintptr_t)segment->data[DATA_COMMITTED] >>
			 region->align_shift) + index;
	key ^= key >> 10;
	key ^= key >> 20;
	return &region->word_locks[key % WORD_LOCK_COUNT];
}

/** Destroy the initialized prefix of a region's word lock table.
 * @param region Region owning the mutexes
 * @param count Number of successfully initialized mutexes
**/
void word_locks_destroy(region_t *region, size_t count);

/** Destroy a fully initialized segment with no concurrent users.
 * @param segment Segment whose buffers and controls are released
**/
void segment_destroy(segment_t *segment);

/** Create a zero-initialized dual-versioned segment in one allocation.
 * @param size Positive segment size, a multiple of align
 * @param align Positive power-of-two word size
 * @param align_shift Precomputed log2(align)
 * @return Initialized segment, or NULL on failure
**/
segment_t *segment_create(size_t size, size_t align, unsigned int align_shift);

/** Find a segment containing a public address in an active transaction's epoch.
 * @param region Region whose published segment list stays stable within the epoch
 * @param transaction Transaction whose private allocations are searched first
 * @param address Public base or interior address to locate
 * @return Accessible segment containing the address, or NULL if none matches
**/
segment_t *transaction_find_segment(const region_t *region,
				    const transaction_t *transaction,
				    const void *address);

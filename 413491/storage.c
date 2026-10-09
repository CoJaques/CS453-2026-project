#include "storage.h"
#include "macros.h"
#include "sync.h"

#include <stdlib.h>

void word_locks_destroy(region_t *region, size_t count)
{
	for (size_t i = 0; i < count; ++i) {
		check_pthread(pthread_mutex_destroy(&region->word_locks[i]));
	}
}

void segment_destroy(segment_t *segment)
{
	// The controls and both copies are interior pointers into this allocation.
	free(segment);
}

segment_t *segment_create(size_t size, size_t align, unsigned int align_shift)
{
	// calloc aligns the base for both structures; round the controls' offset too.
	size_t status_align = _Alignof(word_status_t);
	size_t status_offset =
		sizeof(segment_t) +
		(status_align - sizeof(segment_t) % status_align) %
			status_align;
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
	segment->data[DATA_COMMITTED] = data;
	segment->data[DATA_PENDING] =
		data + size; // size is a multiple of align.
	// Zeroed controls mean unwritten, epoch 0 and ACCESS_NONE.
	// owner is ignored in ACCESS_NONE, so no per-word initialization is needed.
	return segment;
}

segment_t *transaction_find_segment(const region_t *region,
				    const transaction_t *transaction,
				    const void *address)
{
	uintptr_t target = (uintptr_t)address;
	for (const segment_ref_t *ref = transaction->segment_allocated; ref;
	     ref = ref->next) {
		uintptr_t base = (uintptr_t)ref->segment->data[DATA_COMMITTED];
		if (target >= base && target - base < ref->segment->size) {
			return ref->segment;
		}
	}
	for (segment_t *segment = region->head; segment;
	     segment = segment->next) {
		uintptr_t base = (uintptr_t)segment->data[DATA_COMMITTED];
		if (target >= base && target - base < segment->size) {
			return segment;
		}
	}
	return NULL;
}

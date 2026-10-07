#include "tm.h"

typedef struct {
} batcher_t;

typedef struct segment_t {
	shared_t data;
	struct segment_t *next;
} segment_t;

typedef struct {
	size_t size;
	size_t align;
	batcher_t *batcher;
	segment_t *head;
} region_t;

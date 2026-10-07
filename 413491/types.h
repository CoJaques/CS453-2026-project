#pragma once

#include "tm.h"
#include "batcher.h"

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

#pragma once

#include <stdlib.h>

/** Abort on an unrecoverable synchronization failure.
 * @param error Return code from a pthread operation
**/
static inline void check_pthread(int error)
{
	if (error != 0) {
		abort();
	}
}

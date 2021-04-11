
/* Serial Quick Sort and Quick Select implementation
 * Tested here:
 * sorting:
 * https://leetcode.com/problems/sort-an-array/
 *
 * quick select:
 * https://leetcode.com/problems/kth-largest-element-in-an-array
 *
 */

#ifndef SERIAL_SORT_H
#define SERIAL_SORT_H

#include <stdio.h>
#include <stdlib.h>

/* TYPES */
typedef int elem;

/* GLOBALS */

void swap(elem* a, elem* b) {
	elem swapvar = *a;
	*a = *b;
	*b = swapvar;
}

/* If a < b, return -1
 *    a > b, return  1
 *    a = b, return  0
 */
int cmp(elem* a, elem* b) {
	if (*a < *b) {
		return -1;
	} else if (*a > *b) {
		return 1;
	} else {
		return 0;
	}
}

/*
 * Partition a subarray about a pivot 
 */
elem* partition(elem* l, elem* r, elem pivot) {
    
    elem* c = l;
    
    while(c <= r) {
        if (cmp(l, &pivot) == -1) {
            ++l;
            if (c < l) {
                c = l;
            }
            continue;
        }
        if (cmp(r, &pivot) == 1) {
            --r;
            continue;
        }
        if (c < l) {
            c = l;
            continue;
        }
        if (cmp(c, &pivot) == -1) {
            swap(c, l); ++l; continue;
        } 
        if (cmp(c, &pivot) == 1) {
            swap(c, r);
            --r;
            continue;
        }
        ++c;
    }
    return r;
}

/**
 * Perform bubble sort on a subarray
 */
void m_bubble_sort(elem* l, elem* r) {
	if (l >= r) { return; }
	for (elem* i = r; i >= l; --i) {
		for (elem* j = l; j < i; ++j) {
			if ((*j) >= (*(j + 1))) {
				swap(j, j + 1);
			} else {
				break;
			}
		}
	}
}
/**
 * Perform quicksort on a subarray
 */
void m_qsort(elem* l, elem* r) {
	if (l >= r) { return; }

	elem* piv = rand() % (r - l) + l;
	piv = partition(l, r, *piv);
	m_qsort(l, piv - 1);
	m_qsort(piv + 1, r);
}

/**
 * Get Kth element of a subarray in sorted order
 * in O(n) time, O(1) space.
 */

elem findKth(elem* l, elem* r, size_t k) {
	if (k > (1 + (r - l))) {
		fprintf(stderr, "ERROR: k(%ld) larger than total size(%ld) bytes(%ld)\n", k, (r - l), (r - l)); 
		exit(EXIT_FAILURE);
	}
	elem* piv;
	while(l < r) {
		piv = rand() % (r - l) + l;
		piv = partition(l, r, *piv);
		if ((piv - l + 1) > k) {
			r = piv - 1;
		} else if ((piv - l + 1) < k) {
			k -= (piv - l + 1);
			l = piv + 1;
		} else {
			return *piv;
		}
	}

	return *l;
}

#endif





















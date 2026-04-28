// ===========================================================
// gdscloud: Cloud Storage Access for GDS Files
//
// cloud_stream.c: Common cloud stream implementation with LRU block cache
//
// Copyright (C) 2026    Xiuwen Zheng
//
// This file is part of gdscloud.
// LGPL-3 License
// ===========================================================

#include "cloud_stream.h"
#include <string.h>
#include <stdlib.h>


// =====================================================================
// Global default cache size
// =====================================================================

static long long g_max_cache_size = CLOUD_MAX_CACHE_SIZE;

void gdscloud_set_max_cache_size(long long size)
{
	if (size > 0)
		g_max_cache_size = size;
}

long long gdscloud_get_max_cache_size(void)
{
	return g_max_cache_size;
}


// =====================================================================
// Block cache implementation (LRU doubly-linked list)
// =====================================================================

void cache_init(BlockCache *bc, long long block_size, long long max_cache_size)
{
	bc->head = NULL;
	bc->tail = NULL;
	bc->num_blocks = 0;
	bc->block_size = (block_size > 0) ? block_size : CLOUD_BLOCK_SIZE;
	bc->max_blocks = (int)(max_cache_size / bc->block_size);
	if (bc->max_blocks < 1) bc->max_blocks = 1;
	bc->total_hits = 0;
	bc->total_misses = 0;
}

void cache_free(BlockCache *bc)
{
	CacheBlock *cur = bc->head;
	while (cur)
	{
		CacheBlock *next = cur->next;
		free(cur->data);
		free(cur);
		cur = next;
	}
	bc->head = bc->tail = NULL;
	bc->num_blocks = 0;
}

/// Move a block to the head of the LRU list (most recently used)
static void cache_promote(BlockCache *bc, CacheBlock *block)
{
	if (block == bc->head) return;  // already at head

	// unlink from current position
	if (block->prev) block->prev->next = block->next;
	if (block->next) block->next->prev = block->prev;
	if (block == bc->tail) bc->tail = block->prev;

	// insert at head
	block->prev = NULL;
	block->next = bc->head;
	if (bc->head) bc->head->prev = block;
	bc->head = block;
	if (!bc->tail) bc->tail = block;
}

/// Evict the least recently used block (tail)
static void cache_evict(BlockCache *bc)
{
	if (!bc->tail) return;
	CacheBlock *victim = bc->tail;
	if (victim->prev)
		victim->prev->next = NULL;
	else
		bc->head = NULL;
	bc->tail = victim->prev;
	free(victim->data);
	free(victim);
	bc->num_blocks--;
}

CacheBlock *cache_get(BlockCache *bc, long long offset)
{
	// linear search (sufficient for typical cache sizes of ~64 blocks)
	CacheBlock *cur = bc->head;
	while (cur)
	{
		if (cur->offset == offset)
		{
		    cache_promote(bc, cur);
		    bc->total_hits++;
		    return cur;
		}
		cur = cur->next;
	}
	bc->total_misses++;
	return NULL;
}

CacheBlock *cache_put(BlockCache *bc, long long offset,
	const unsigned char *data, long long valid_size)
{
	// evict if at capacity
	while (bc->num_blocks >= bc->max_blocks)
		cache_evict(bc);

	// allocate new block
	CacheBlock *block = (CacheBlock *)calloc(1, sizeof(CacheBlock));
	if (!block) return NULL;
	block->data = (unsigned char *)malloc(bc->block_size);
	if (!block->data)
	{
		free(block);
		return NULL;
	}

	block->offset = offset;
	block->valid_size = valid_size;
	memcpy(block->data, data, (size_t)valid_size);

	// insert at head
	block->prev = NULL;
	block->next = bc->head;
	if (bc->head) bc->head->prev = block;
	bc->head = block;
	if (!bc->tail) bc->tail = block;
	bc->num_blocks++;

	return block;
}

void cache_clear_all(BlockCache *bc)
{
	long long bs = bc->block_size;
	int mb = bc->max_blocks;
	cache_free(bc);
	bc->block_size = bs;
	bc->max_blocks = mb;
	bc->total_hits = 0;
	bc->total_misses = 0;
}


// =====================================================================
// Cloud stream implementation
// =====================================================================

CloudStream *cloud_stream_create(const char *url,
	CloudBackend *backend, void *backend_data,
	long long block_size, long long max_cache_size)
{
	CloudStream *cs = (CloudStream *)calloc(1, sizeof(CloudStream));
	if (!cs) return NULL;

	// copy URL
	strncpy(cs->url, url, CLOUD_MAX_URL_LEN - 1);
	cs->url[CLOUD_MAX_URL_LEN - 1] = '\0';

	cs->file_size = -1;  // unknown until first query
	cs->position = 0;
	cs->backend = *backend;
	cs->backend_data = backend_data;

	// initialize cache
	if (block_size <= 0) block_size = CLOUD_BLOCK_SIZE;
	if (max_cache_size <= 0) max_cache_size = g_max_cache_size;
	cache_init(&cs->cache, block_size, max_cache_size);

	return cs;
}

long long cloud_stream_read(CloudStream *cs, void *buffer, long long count)
{
	if (!cs || count <= 0) return 0;

	// ensure file size is known
	if (cs->file_size < 0)
	{
		cs->file_size = cs->backend.get_size(cs->backend_data, cs->url);
		if (cs->file_size < 0) return -1;
	}

	// clamp to remaining bytes
	if (cs->position >= cs->file_size) return 0;
	if (cs->position + count > cs->file_size)
		count = cs->file_size - cs->position;

	long long total_read = 0;
	unsigned char *out = (unsigned char *)buffer;

	while (count > 0)
	{
		// which cache block does position fall in?
		long long block_offset = (cs->position / cs->cache.block_size)
		    * cs->cache.block_size;
		long long offset_in_block = cs->position - block_offset;

		CacheBlock *blk = cache_get(&cs->cache, block_offset);
		if (!blk)
		{
		    // fetch from backend
		    long long fetch_size = cs->cache.block_size;
		    if (block_offset + fetch_size > cs->file_size)
		        fetch_size = cs->file_size - block_offset;

		    unsigned char *tmpbuf = (unsigned char *)malloc((size_t)fetch_size);
		    if (!tmpbuf) return (total_read > 0) ? total_read : -1;

		    long long got = cs->backend.read_range(cs->backend_data,
		        cs->url, block_offset, fetch_size, tmpbuf);
		    if (got <= 0)
		    {
		        free(tmpbuf);
		        return (total_read > 0) ? total_read : -1;
		    }

		    blk = cache_put(&cs->cache, block_offset, tmpbuf, got);
		    free(tmpbuf);
		    if (!blk) return (total_read > 0) ? total_read : -1;
		}

		// copy available bytes from this block
		long long avail = blk->valid_size - offset_in_block;
		if (avail <= 0) break;
		if (avail > count) avail = count;

		memcpy(out, blk->data + offset_in_block, (size_t)avail);
		out += avail;
		cs->position += avail;
		total_read += avail;
		count -= avail;
	}

	return total_read;
}

long long cloud_stream_seek(CloudStream *cs, long long offset, int origin)
{
	if (!cs) return -1;

	// ensure file size is known for soEnd
	if (cs->file_size < 0 && origin == 2)
	{
		cs->file_size = cs->backend.get_size(cs->backend_data, cs->url);
		if (cs->file_size < 0) return -1;
	}

	long long new_pos;
	switch (origin)
	{
		case 0:  // soBeginning
		    new_pos = offset;
		    break;
		case 1:  // soCurrent
		    new_pos = cs->position + offset;
		    break;
		case 2:  // soEnd
		    new_pos = cs->file_size + offset;
		    break;
		default:
		    return -1;
	}

	if (new_pos < 0) new_pos = 0;
	cs->position = new_pos;
	return cs->position;
}

long long cloud_stream_getsize(CloudStream *cs)
{
	if (!cs) return -1;
	if (cs->file_size < 0)
	{
		cs->file_size = cs->backend.get_size(cs->backend_data, cs->url);
	}
	return cs->file_size;
}

void cloud_stream_close(CloudStream *cs)
{
	if (!cs) return;
	cache_free(&cs->cache);
	if (cs->backend.close)
		cs->backend.close(cs->backend_data);
	free(cs);
}

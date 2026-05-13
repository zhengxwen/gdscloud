// ===========================================================
// gdscloud: Cloud Storage Access for GDS Files
//
// cloud_stream.c: Common cloud stream implementation with LRU block cache
//
// Copyright (C) 2026    Xiuwen Zheng
//
// This file is part of gdscloud.
// GPL-3 License
// ===========================================================

#include "cloud_stream.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>


// =====================================================================
// Fork safety (Unix only)
// =====================================================================

#ifndef _WIN32
static pid_t g_curl_init_pid = 0;
#endif

void cloud_init_fork_tracking(void)
{
#ifndef _WIN32
	g_curl_init_pid = getpid();
#endif
}

int cloud_check_reinit_curl(CURL **curl_ptr)
{
#ifndef _WIN32
	pid_t cur_pid = getpid();
	if (g_curl_init_pid != 0 && cur_pid != g_curl_init_pid)
	{
		// We are in a forked child process.
		// Re-initialize libcurl global state (safe to call again per docs).
		curl_global_init(CURL_GLOBAL_ALL);
		// Destroy the inherited (unsafe) handle and create a fresh one.
		if (curl_ptr && *curl_ptr)
		{
			curl_easy_cleanup(*curl_ptr);
			*curl_ptr = curl_easy_init();
		}
		g_curl_init_pid = cur_pid;
		return 1;
	}
#endif
	return 0;
}


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
// Error formatting helper
// =====================================================================

void cloud_format_error(char *out, size_t out_size,
	const char *prefix, const char *endpoint,
	CURLcode res, long http_code,
	const unsigned char *body, long long body_len)
{
	if (!out || out_size == 0) return;
	out[0] = '\0';

	if (res != CURLE_OK)
	{
		snprintf(out, out_size, "%s: curl error accessing '%s': %s",
			prefix, endpoint, curl_easy_strerror(res));
		return;
	}

	// truncate body at 1024 chars
	if (body && body_len > 0)
	{
		char body_snippet[1025];
		long long copy_len = (body_len > 1024) ? 1024 : body_len;
		memcpy(body_snippet, body, (size_t)copy_len);
		body_snippet[copy_len] = '\0';
		if (body_len > 1024)
		{
			snprintf(out, out_size,
				"%s: HTTP %ld accessing '%s'. Server response: %s... [truncated]",
				prefix, http_code, endpoint, body_snippet);
		} else {
			snprintf(out, out_size,
				"%s: HTTP %ld accessing '%s'. Server response: %s",
				prefix, http_code, endpoint, body_snippet);
		}
	}
	else
	{
		snprintf(out, out_size, "%s: HTTP %ld accessing '%s'",
			prefix, http_code, endpoint);
	}
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

#ifndef _WIN32
	cs->creator_pid = getpid();
#endif

	// initialize cache
	if (block_size <= 0) block_size = CLOUD_BLOCK_SIZE;
	if (max_cache_size <= 0) max_cache_size = g_max_cache_size;
	cache_init(&cs->cache, block_size, max_cache_size);

	return cs;
}

long long cloud_stream_read(CloudStream *cs, void *buffer, long long count)
{
	if (!cs || count <= 0) return 0;

#ifndef _WIN32
	// Fork detection: if we are in a child process, clear stale cache
	// and force re-query of file size through the fresh CURL handle.
	if (cs->creator_pid != 0 && getpid() != cs->creator_pid)
	{
		cache_clear_all(&cs->cache);
		cs->file_size = -1;
		cs->creator_pid = getpid();
	}
#endif

	// ensure file size is known
	if (cs->file_size < 0)
	{
		cs->file_size = cs->backend.get_size(cs->backend_data, cs->url);
		if (cs->file_size < 0)
		{
			if (cs->backend.get_last_error)
			{
				const char *err = cs->backend.get_last_error(cs->backend_data);
				if (err && err[0])
				{
					strncpy(cs->last_error, err, CLOUD_MAX_ERROR_LEN - 1);
					cs->last_error[CLOUD_MAX_ERROR_LEN - 1] = '\0';
				}
			}
			return -1;
		}
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
				if (cs->backend.get_last_error)
				{
					const char *err = cs->backend.get_last_error(cs->backend_data);
					if (err && err[0])
					{
						strncpy(cs->last_error, err, CLOUD_MAX_ERROR_LEN - 1);
						cs->last_error[CLOUD_MAX_ERROR_LEN - 1] = '\0';
					}
				}
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
		if (cs->file_size < 0)
		{
			if (cs->backend.get_last_error)
			{
				const char *err = cs->backend.get_last_error(cs->backend_data);
				if (err && err[0])
				{
					strncpy(cs->last_error, err, CLOUD_MAX_ERROR_LEN - 1);
					cs->last_error[CLOUD_MAX_ERROR_LEN - 1] = '\0';
				}
			}
			return -1;
		}
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
		if (cs->file_size < 0 && cs->backend.get_last_error)
		{
			const char *err = cs->backend.get_last_error(cs->backend_data);
			if (err && err[0])
			{
				strncpy(cs->last_error, err, CLOUD_MAX_ERROR_LEN - 1);
				cs->last_error[CLOUD_MAX_ERROR_LEN - 1] = '\0';
			}
		}
	}
	return cs->file_size;
}

const char *cloud_stream_get_last_error(CloudStream *cs)
{
	if (!cs) return "";
	return cs->last_error;
}

void cloud_stream_close(CloudStream *cs)
{
	if (!cs) return;
	cache_free(&cs->cache);
	if (cs->backend.close)
		cs->backend.close(cs->backend_data);
	free(cs);
}

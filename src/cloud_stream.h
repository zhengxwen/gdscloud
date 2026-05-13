// ===========================================================
// gdscloud: Cloud Storage Access for GDS Files
//
// cloud_stream.h: Common cloud stream interface with block cache
//
// Copyright (C) 2026    Xiuwen Zheng
//
// This file is part of gdscloud.
// GPL-3 License
// ===========================================================

#ifndef GDSCLOUD_CLOUD_STREAM_H
#define GDSCLOUD_CLOUD_STREAM_H

#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>

#ifndef _WIN32
#include <unistd.h>
#include <sys/types.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif


// =====================================================================
// Block cache configuration
// =====================================================================

/// Default block size: 1 MB
#define CLOUD_BLOCK_SIZE        (1024 * 1024)
/// Default max cache size: 64 MB
#define CLOUD_MAX_CACHE_SIZE    (64 * 1024 * 1024)
/// Maximum URL length
#define CLOUD_MAX_URL_LEN       4096
/// Maximum credential string length
#define CLOUD_MAX_CRED_LEN      1024
/// Maximum error message length
#define CLOUD_MAX_ERROR_LEN     4096


// =====================================================================
// Cache block structure (LRU linked list)
// =====================================================================

typedef struct CacheBlock {
	long long offset;           // file offset this block covers
	long long valid_size;       // actual valid bytes in this block
	unsigned char *data;        // block data buffer (CLOUD_BLOCK_SIZE)
	struct CacheBlock *prev;    // LRU: more recently used
	struct CacheBlock *next;    // LRU: less recently used
} CacheBlock;

typedef struct BlockCache {
	CacheBlock *head;           // most recently used
	CacheBlock *tail;           // least recently used
	int num_blocks;             // current number of blocks
	int max_blocks;             // max blocks allowed
	long long block_size;       // size of each block
	long long total_hits;       // cache hit count
	long long total_misses;     // cache miss count
} BlockCache;


// =====================================================================
// Backend vtable (each cloud provider implements these)
// =====================================================================

typedef struct CloudBackend {
	/// Read a byte range from the URL; return bytes read, or -1 on error
	long long (*read_range)(void *backend_data, const char *url,
		long long offset, long long length, unsigned char *buffer);
	/// Get total file size; return size, or -1 on error
	long long (*get_size)(void *backend_data, const char *url);
	/// Free backend resources
	void (*close)(void *backend_data);
	/// Get last error message; may return NULL or empty string
	const char *(*get_last_error)(void *backend_data);
} CloudBackend;


// =====================================================================
// Cloud stream structure (passed as user_data to gdsfmt callbacks)
// =====================================================================

typedef struct CloudStream {
	char url[CLOUD_MAX_URL_LEN];    // resolved HTTP(S) URL
	long long file_size;             // total file size (-1 if unknown)
	long long position;              // current read position
	BlockCache cache;                // block cache
	CloudBackend backend;            // backend function pointers
	void *backend_data;              // backend-specific state
	char last_error[CLOUD_MAX_ERROR_LEN];  // last error message
#ifndef _WIN32
	pid_t creator_pid;               // pid at creation time (fork detection)
#endif
} CloudStream;


// =====================================================================
// Cloud stream API
// =====================================================================

/// Create a new cloud stream
CloudStream *cloud_stream_create(const char *url,
	CloudBackend *backend, void *backend_data,
	long long block_size, long long max_cache_size);

/// Read from the cloud stream (updates position)
long long cloud_stream_read(CloudStream *cs, void *buffer, long long count);

/// Seek within the cloud stream
long long cloud_stream_seek(CloudStream *cs, long long offset, int origin);

/// Get the file size
long long cloud_stream_getsize(CloudStream *cs);

/// Close and free the cloud stream
void cloud_stream_close(CloudStream *cs);

/// Get the last error message (empty string if none)
const char *cloud_stream_get_last_error(CloudStream *cs);

/// Format a cloud backend error message (helper for backends)
void cloud_format_error(char *out, size_t out_size,
	const char *prefix, const char *endpoint,
	CURLcode res, long http_code,
	const unsigned char *body, long long body_len);


// =====================================================================
// Block cache API (internal)
// =====================================================================

void cache_init(BlockCache *bc, long long block_size, long long max_cache_size);
void cache_free(BlockCache *bc);
CacheBlock *cache_get(BlockCache *bc, long long offset);
CacheBlock *cache_put(BlockCache *bc, long long offset, const unsigned char *data,
	long long valid_size);
void cache_clear_all(BlockCache *bc);


// =====================================================================
// Global cache size setting
// =====================================================================

/// Set the default max cache size for new streams (in bytes)
void gdscloud_set_max_cache_size(long long size);
/// Get the current default max cache size
long long gdscloud_get_max_cache_size(void);


// =====================================================================
// Fork safety (Unix only)
// =====================================================================

/// Initialize fork tracking (call once after curl_global_init)
void cloud_init_fork_tracking(void);

/// Check if we are in a forked child; if so, reinitialize the CURL handle.
/// Returns 1 if reinitialized, 0 if no action taken.
int cloud_check_reinit_curl(CURL **curl_ptr);


#ifdef __cplusplus
}
#endif

#endif /* GDSCLOUD_CLOUD_STREAM_H */

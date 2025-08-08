// hash-table-v2-persistent-caches.c
#include "hash-table-base.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>  // optional - can remove if you don't want debug prints

#define CACHE_SIZE 8

// --- Per-thread cache structures (heap-allocated) ---
struct cache_entry {
    char *key;
    uint32_t value;
};

struct thread_cache {
    struct cache_entry entries[CACHE_SIZE];
    size_t count;
    pthread_mutex_t lock; // Protects this cache
    bool dirty; // True if unflushed entries exist
    SLIST_ENTRY(thread_cache) pointers; // For master list
};

// --- Hash table structures ---
struct list_entry {
    const char *key;
    uint32_t value;
    SLIST_ENTRY(list_entry) pointers;
};

SLIST_HEAD(list_head, list_entry);

struct hash_table_entry {
    struct list_head list_head;
    pthread_mutex_t mutex;
};

struct hash_table_v2 {
    struct hash_table_entry entries[HASH_TABLE_CAPACITY];
};

// --- Global cache registry ---
static pthread_mutex_t master_lock = PTHREAD_MUTEX_INITIALIZER;
static SLIST_HEAD(master_cache_list, thread_cache) master_list =
    SLIST_HEAD_INITIALIZER(master_list);

// --- Thread-local pointer to the heap-allocated cache ---
static _Thread_local struct thread_cache *tls_cache = NULL;

// --- Helper functions ---

// Allocate and register a thread cache (idempotent for a thread).
static void register_thread_cache() {
    if (tls_cache != NULL) return; // already registered for this thread

    struct thread_cache *cache = calloc(1, sizeof(*cache));
    if (!cache) {
        // Allocation failure: abort as it's hard to proceed safely.
        abort();
    }

    // Initialize cache fields
    cache->count = 0;
    cache->dirty = false;
    for (size_t i = 0; i < CACHE_SIZE; i++) {
        cache->entries[i].key = NULL;
        cache->entries[i].value = 0;
    }
    if (pthread_mutex_init(&cache->lock, NULL) != 0) {
        free(cache);
        abort();
    }

    // Register in the master list under master_lock
    if (pthread_mutex_lock(&master_lock) != 0) {
        // Can't lock master - cleanup and abort
        pthread_mutex_destroy(&cache->lock);
        free(cache);
        abort();
    }
    SLIST_INSERT_HEAD(&master_list, cache, pointers);
    pthread_mutex_unlock(&master_lock);

    // Save pointer in TLS for quick access
    tls_cache = cache;
}

// Flush one thread_cache into the hash table. Locks cache->lock while operating.
static void flush_thread_cache(struct hash_table_v2 *ht, struct thread_cache *cache) {
    if (!ht || !cache) return;

    if (pthread_mutex_lock(&cache->lock) != 0) {
        // Could not lock cached lock; treat as no-op
        return;
    }

    for (size_t i = 0; i < cache->count; i++) {
        char *entry_key = cache->entries[i].key;
        uint32_t value = cache->entries[i].value;

        if (!entry_key) continue;

        uint32_t index = bernstein_hash(entry_key) % HASH_TABLE_CAPACITY;

        // Lock bucket mutex to update/insert
        pthread_mutex_lock(&ht->entries[index].mutex);
        struct list_entry *le;
        SLIST_FOREACH(le, &ht->entries[index].list_head, pointers) {
            if (strcmp(le->key, entry_key) == 0) {
                // Update existing entry
                le->value = value;
                free((void*)entry_key); // free the duplicated key from cache
                cache->entries[i].key = NULL;
                goto unlock_bucket;
            }
        }
        // Not found -> insert new list_entry, transfer ownership of key
        le = malloc(sizeof(*le));
        if (!le) {
            // allocation failed: free cache key and skip
            free((void*)entry_key);
            cache->entries[i].key = NULL;
            pthread_mutex_unlock(&ht->entries[index].mutex);
            continue;
        }
        le->key = entry_key; // transfer ownership
        le->value = value;
        SLIST_INSERT_HEAD(&ht->entries[index].list_head, le, pointers);

    unlock_bucket:
        pthread_mutex_unlock(&ht->entries[index].mutex);
    }

    cache->count = 0;
    cache->dirty = false;
    pthread_mutex_unlock(&cache->lock);
}

// Flush all caches registered in master_list.
// We copy the pointers under master_lock, then flush each cache without holding master_lock
// to avoid lock-order inversion between master_lock and per-cache locks.
static void flush_all_caches(struct hash_table_v2 *ht) {
    if (!ht) return;

    if (pthread_mutex_lock(&master_lock) != 0) return;

    // Count caches
    size_t count = 0;
    struct thread_cache *tc;
    SLIST_FOREACH(tc, &master_list, pointers) count++;

    struct thread_cache **arr = NULL;
    if (count > 0) {
        arr = malloc(sizeof(*arr) * count);
        if (!arr) {
            pthread_mutex_unlock(&master_lock);
            return;
        }
    }

    size_t idx = 0;
    SLIST_FOREACH(tc, &master_list, pointers) {
        arr[idx++] = tc;
    }

    pthread_mutex_unlock(&master_lock);

    // Flush each cache outside the master_lock
    for (size_t i = 0; i < idx; i++) {
        flush_thread_cache(ht, arr[i]);
    }

    free(arr);
}

// --- Public API ---
struct hash_table_v2 *hash_table_v2_create() {
    struct hash_table_v2 *ht = calloc(1, sizeof(*ht));
    if (!ht) return NULL;

    for (size_t i = 0; i < HASH_TABLE_CAPACITY; i++) {
        SLIST_INIT(&ht->entries[i].list_head);
        if (pthread_mutex_init(&ht->entries[i].mutex, NULL) != 0) {
            // cleanup previous in case of failure
            for (size_t j = 0; j < i; j++) {
                pthread_mutex_destroy(&ht->entries[j].mutex);
            }
            free(ht);
            return NULL;
        }
    }

    // Note: do NOT register a cache for the creating thread automatically.
    // Threads will register lazily on first use.
    return ht;
}

void hash_table_v2_add_entry(struct hash_table_v2 *ht, const char *key, uint32_t value) {
    if (!ht || !key) return;

    // Ensure this thread has a registered cache
    if (!tls_cache) register_thread_cache();
    struct thread_cache *cache = tls_cache;

    // Lock per-thread cache to append; if full, flush first (without holding lock during flush)
    if (pthread_mutex_lock(&cache->lock) != 0) {
        return;
    }

    if (cache->count == CACHE_SIZE) {
        // release lock then flush; flush will lock the cache itself
        pthread_mutex_unlock(&cache->lock);
        flush_thread_cache(ht, cache);
        // Re-lock to add new entry
        if (pthread_mutex_lock(&cache->lock) != 0) {
            return;
        }
    }

    char *key_copy = strdup(key);
    if (!key_copy) {
        pthread_mutex_unlock(&cache->lock);
        return;
    }

    cache->entries[cache->count++] = (struct cache_entry){
        .key = key_copy,
        .value = value
    };
    cache->dirty = true;
    pthread_mutex_unlock(&cache->lock);
}

bool hash_table_v2_contains(struct hash_table_v2 *ht, const char *key) {
    if (!ht || !key) return false;

    // If this thread has a cache and it's dirty, flush it so contains sees latest entries
    flush_all_caches(ht);

    uint32_t index = bernstein_hash(key) % HASH_TABLE_CAPACITY;
    pthread_mutex_lock(&ht->entries[index].mutex);

    bool found = false;
    struct list_entry *le;
    size_t iter = 0;
    const size_t max_iter = 10000;
    SLIST_FOREACH(le, &ht->entries[index].list_head, pointers) {
        if (++iter > max_iter) break; // guard against corruption
        if (strcmp(le->key, key) == 0) {
            found = true;
            break;
        }
    }

    pthread_mutex_unlock(&ht->entries[index].mutex);
    return found;
}

void hash_table_v2_destroy(struct hash_table_v2 *ht) {
    if (!ht) return;

    // Flush all caches (including caches from threads that already exited).
    flush_all_caches(ht);

    // Copy master_list pointers under master_lock, then clear master_list so future registrations
    // (if any) won't see stale entries. After we drop master_lock we can safely clean each cache.
    if (pthread_mutex_lock(&master_lock) != 0) {
        // If we cannot lock master_lock we attempt best-effort cleanup of buckets only.
    } else {
        size_t count = 0;
        struct thread_cache *tc;
        SLIST_FOREACH(tc, &master_list, pointers) count++;

        struct thread_cache **arr = NULL;
        if (count > 0) {
            arr = malloc(sizeof(*arr) * count);
            if (!arr) {
                // Can't allocate array: unlock and skip per-cache cleanup.
                pthread_mutex_unlock(&master_lock);
                arr = NULL;
                count = 0;
            } else {
                size_t i = 0;
                SLIST_FOREACH(tc, &master_list, pointers) {
                    arr[i++] = tc;
                }
                // Clear master list now
                SLIST_INIT(&master_list);
                pthread_mutex_unlock(&master_lock);

                // Now clean up each cache (lock, free keys, unlock, destroy mutex, free cache)
                for (size_t j = 0; j < i; j++) {
                    struct thread_cache *c = arr[j];
                    if (!c) continue;
                    // Lock to safely access entries
                    if (pthread_mutex_lock(&c->lock) == 0) {
                        for (size_t k = 0; k < c->count; k++) {
                            free((void*)c->entries[k].key);
                            c->entries[k].key = NULL;
                        }
                        c->count = 0;
                        c->dirty = false;
                        pthread_mutex_unlock(&c->lock);
                    }
                    // Destroy the cache lock and free the cache itself.
                    pthread_mutex_destroy(&c->lock);
                    free(c);
                }
                free(arr);
            }
        } else {
            // Nothing to clean; just unlock
            pthread_mutex_unlock(&master_lock);
        }
    }

    // Free hash table buckets and their list entries
    for (size_t i = 0; i < HASH_TABLE_CAPACITY; i++) {
        struct list_entry *le;
        while (!SLIST_EMPTY(&ht->entries[i].list_head)) {
            le = SLIST_FIRST(&ht->entries[i].list_head);
            SLIST_REMOVE_HEAD(&ht->entries[i].list_head, pointers);
            free((void*)le->key);
            free(le);
        }
        pthread_mutex_destroy(&ht->entries[i].mutex);
    }

    free(ht);
}

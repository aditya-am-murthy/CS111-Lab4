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

#define CACHE_SIZE 4

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
    if (tls_cache && tls_cache->dirty) {
        flush_thread_cache(ht, tls_cache);
    }

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

    pthread_mutex_lock(&master_lock);
    struct thread_cache *cache;
    int cache_index = 0;
    SLIST_FOREACH(cache, &master_list, pointers) {
        pthread_mutex_lock(&cache->lock);

        printf("[Destroy] Cache %d contents before flush:\n", cache_index);
        for (size_t i = 0; i < cache->count; i++) {
            printf("  -> %s\n", cache->entries[i].key);
        }

        pthread_mutex_unlock(&cache->lock);
        cache_index++;
    }
    pthread_mutex_unlock(&master_lock);

    // Now flush all caches
    flush_all_caches(ht);

    // Destroy the hash table's buckets
    for (size_t i = 0; i < ht->size; i++) {
        struct entry *curr = ht->buckets[i];
        while (curr) {
            struct entry *tmp = curr;
            curr = curr->next;
            free(tmp->key);
            free(tmp);
        }
    }

    free(ht->buckets);
    pthread_mutex_destroy(&ht->table_lock);

    // Free caches themselves
    pthread_mutex_lock(&master_lock);
    while (!SLIST_EMPTY(&master_list)) {
        cache = SLIST_FIRST(&master_list);
        SLIST_REMOVE_HEAD(&master_list, pointers);
        pthread_mutex_destroy(&cache->lock);
        free(cache);
    }
    pthread_mutex_unlock(&master_lock);
}

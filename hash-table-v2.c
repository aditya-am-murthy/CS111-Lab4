// corrected-hash-table-v2.c
#include "hash-table-base.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>  // optional - remove if you don't want debug prints

#define CACHE_SIZE 4

// --- Per-thread cache structures ---
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

// --- Thread-local cache (per-thread instance) ---
// NOTE: renamed to avoid confusion with type name
static _Thread_local struct thread_cache tls_cache = {
    .count = 0,
    .dirty = false
};
// Per-thread registered flag to avoid duplicate registration
static _Thread_local bool tls_cache_registered = false;

// --- Helper functions ---

// Register the current thread's tls_cache in the global master_list.
// Safe to call multiple times from same thread (idempotent).
static void register_thread_cache() {
    if (tls_cache_registered) return;

    if (pthread_mutex_lock(&master_lock) != 0) {
        abort();
    }

    // Initialize thread cache fields (only once per thread)
    tls_cache.count = 0;
    tls_cache.dirty = false;

    if (pthread_mutex_init(&tls_cache.lock, NULL) != 0) {
        pthread_mutex_unlock(&master_lock);
        abort();
    }
    for (size_t i = 0; i < CACHE_SIZE; i++) {
        tls_cache.entries[i].key = NULL;
        tls_cache.entries[i].value = 0;
    }

    // Insert into master list (protected by master_lock)
    SLIST_INSERT_HEAD(&master_list, &tls_cache, pointers);
    tls_cache_registered = true;

    pthread_mutex_unlock(&master_lock);
}

// Flush a single thread cache into the provided hash table.
// This locks cache->lock while processing the cache entries.
// It locks the destination bucket mutex before modifying that bucket.
static void flush_thread_cache(struct hash_table_v2 *ht, struct thread_cache *cache) {
    if (!cache || !ht) return;

    if (pthread_mutex_lock(&cache->lock) != 0) {
        // can't lock - treat as no-op
        return;
    }

    for (size_t i = 0; i < cache->count; i++) {
        char *entry_key = cache->entries[i].key;
        uint32_t value = cache->entries[i].value;

        if (!entry_key) continue;

        uint32_t index = bernstein_hash(entry_key) % HASH_TABLE_CAPACITY;

        // Lock the hash bucket, then either update existing entry or insert new one.
        pthread_mutex_lock(&ht->entries[index].mutex);
        struct list_entry *le = NULL;
        SLIST_FOREACH(le, &ht->entries[index].list_head, pointers) {
            if (strcmp(le->key, entry_key) == 0) {
                // Update existing value, free the cache's key copy
                le->value = value;
                free((void*)entry_key);
                cache->entries[i].key = NULL;
                goto unlocked_bucket;
            }
        }
        // Not found - create new list_entry and transfer ownership of key to it
        le = malloc(sizeof(*le));
        if (!le) {
            // Allocation failure: free cache key and skip
            free((void*)entry_key);
            cache->entries[i].key = NULL;
            pthread_mutex_unlock(&ht->entries[index].mutex);
            continue;
        }
        le->key = entry_key;
        le->value = value;
        SLIST_INSERT_HEAD(&ht->entries[index].list_head, le, pointers);

    unlocked_bucket:
        pthread_mutex_unlock(&ht->entries[index].mutex);
    }

    cache->count = 0;
    cache->dirty = false;
    pthread_mutex_unlock(&cache->lock);
}

// Flush all registered thread caches into the hash table.
//
// Important: we avoid holding master_lock while flushing each cache (which locks
// cache->lock) to prevent lock order inversion. We copy pointers out under
// master_lock, then flush each cache without holding master_lock.
static void flush_all_caches(struct hash_table_v2 *ht) {
    if (!ht) return;

    // First, copy the cache pointers under master_lock to a dynamic array.
    if (pthread_mutex_lock(&master_lock) != 0) {
        return;
    }

    size_t count = 0;
    struct thread_cache *cache;
    SLIST_FOREACH(cache, &master_list, pointers) {
        count++;
    }

    struct thread_cache **cache_array = NULL;
    if (count > 0) {
        cache_array = malloc(sizeof(*cache_array) * count);
        if (!cache_array) {
            // allocation failure — we can't safely flush all caches; just unlock and return
            pthread_mutex_unlock(&master_lock);
            return;
        }
    }

    size_t idx = 0;
    SLIST_FOREACH(cache, &master_list, pointers) {
        cache_array[idx++] = cache;
    }

    pthread_mutex_unlock(&master_lock);

    // Now flush each cache without holding master_lock.
    for (size_t i = 0; i < idx; i++) {
        flush_thread_cache(ht, cache_array[i]);
    }

    free(cache_array);
}

// --- Public API ---
struct hash_table_v2 *hash_table_v2_create() {
    struct hash_table_v2 *ht = calloc(1, sizeof(*ht));
    if (!ht) {
        return NULL;
    }
    for (size_t i = 0; i < HASH_TABLE_CAPACITY; i++) {
        SLIST_INIT(&ht->entries[i].list_head);
        if (pthread_mutex_init(&ht->entries[i].mutex, NULL) != 0) {
            // Clean up previously created mutexes
            for (size_t j = 0; j < i; j++) {
                pthread_mutex_destroy(&ht->entries[j].mutex);
            }
            free(ht);
            return NULL;
        }
    }

    // Do not force registration of the creating thread here.
    // Each thread will register itself on first use (lazy registration).
    return ht;
}

void hash_table_v2_add_entry(struct hash_table_v2 *ht, const char *key, uint32_t value) {
    if (!ht || !key) return;

    // Register cache on first use for this thread (idempotent).
    if (!tls_cache_registered) {
        register_thread_cache();
    }

    // Acquire the per-thread cache lock and append or flush if full.
    if (pthread_mutex_lock(&tls_cache.lock) != 0) {
        // can't lock: give up this entry
        return;
    }

    if (tls_cache.count == CACHE_SIZE) {
        // release the per-thread lock before flushing, to avoid deadlock in flush
        pthread_mutex_unlock(&tls_cache.lock);
        flush_thread_cache(ht, &tls_cache);
        if (pthread_mutex_lock(&tls_cache.lock) != 0) {
            return;
        }
    }

    char *key_copy = strdup(key);
    if (!key_copy) {
        pthread_mutex_unlock(&tls_cache.lock);
        return;
    }

    tls_cache.entries[tls_cache.count++] = (struct cache_entry){
        .key = key_copy,
        .value = value
    };
    tls_cache.dirty = true;
    pthread_mutex_unlock(&tls_cache.lock);
}

bool hash_table_v2_contains(struct hash_table_v2 *ht, const char *key) {
    if (!ht || !key) return false;

    // Ensure current thread's cache is flushed (so we check global state).
    if (tls_cache_registered && tls_cache.dirty) {
        flush_thread_cache(ht, &tls_cache);
    }

    uint32_t index = bernstein_hash(key) % HASH_TABLE_CAPACITY;
    pthread_mutex_lock(&ht->entries[index].mutex);

    bool found = false;
    struct list_entry *le;
    size_t iteration_count = 0;
    const size_t max_iterations = 10000; // Safeguard against list corruption
    SLIST_FOREACH(le, &ht->entries[index].list_head, pointers) {
        if (++iteration_count > max_iterations) {
            // Potential corruption; break out
            break;
        }
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

    // Flush all caches first (this copies master list then flushes).
    flush_all_caches(ht);

    // To clean up per-thread cache mutexes and entries, copy the pointers
    // under master_lock, then clean them up without holding master_lock.
    if (pthread_mutex_lock(&master_lock) != 0) {
        // If we can't lock master_lock, skip cleanup — but still attempt to free table buckets.
    } else {
        size_t count = 0;
        struct thread_cache *cache;
        SLIST_FOREACH(cache, &master_list, pointers) {
            count++;
        }

        struct thread_cache **cache_array = NULL;
        if (count > 0) {
            cache_array = malloc(sizeof(*cache_array) * count);
            if (!cache_array) {
                // Cleanup fallback: do not attempt per-cache cleanup
                pthread_mutex_unlock(&master_lock);
                cache_array = NULL;
                count = 0;
            } else {
                size_t i = 0;
                SLIST_FOREACH(cache, &master_list, pointers) {
                    cache_array[i++] = cache;
                }
                // Clear master list so subsequent register attempts won't see stale entries.
                SLIST_INIT(&master_list);
                pthread_mutex_unlock(&master_lock);

                // Now for each cache: lock it, free entries, unlock, destroy mutex.
                // NOTE: The caller must ensure no other threads are concurrently using the
                // cache API. Destroying mutexes while other threads might still try to lock
                // them is undefined behavior.
                for (size_t j = 0; j < i; j++) {
                    struct thread_cache *tc = cache_array[j];
                    if (!tc) continue;

                    // Lock, free keys, unlock, then destroy mutex.
                    if (pthread_mutex_lock(&tc->lock) == 0) {
                        for (size_t k = 0; k < tc->count; k++) {
                            free((void*)tc->entries[k].key);
                            tc->entries[k].key = NULL;
                        }
                        tc->count = 0;
                        tc->dirty = false;
                        pthread_mutex_unlock(&tc->lock);
                    }
                    // Now it's safe to destroy the mutex (caller must ensure no concurrent use).
                    pthread_mutex_destroy(&tc->lock);
                }
                free(cache_array);
                cache_array = NULL;
            }
        } else {
            // No caches to clean, just unlock.
            pthread_mutex_unlock(&master_lock);
        }
    }

    // Free hash table buckets and entries
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

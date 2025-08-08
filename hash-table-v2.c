#include "hash-table-base.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <pthread.h>

#define CACHE_SIZE 4

// --- Per-thread cache structures ---
struct cache_entry {
    char *key;
    uint32_t value;
};

struct thread_cache {
    struct cache_entry entries[CACHE_SIZE];
    size_t count;
    pthread_mutex_t lock;  // Protects this cache
    bool dirty;           // True if unflushed entries exist
    SLIST_ENTRY(thread_cache) pointers;  // For master list
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

// --- Thread-local cache ---
static _Thread_local struct thread_cache thread_cache = {
    .count = 0,
    .lock = PTHREAD_MUTEX_INITIALIZER,
    .dirty = false
};

// --- Helper functions ---
static void register_thread_cache() {
    pthread_mutex_lock(&master_lock);
    SLIST_INSERT_HEAD(&master_list, &thread_cache, pointers);
    pthread_mutex_unlock(&master_lock);
}

static void flush_thread_cache(struct hash_table_v2 *ht, 
                             struct thread_cache *cache) {
    pthread_mutex_lock(&cache->lock);
    for (size_t i = 0; i < cache->count; i++) {
        const char *key = cache->entries[i].key;
        uint32_t value = cache->entries[i].value;
        uint32_t index = bernstein_hash(key) % HASH_TABLE_CAPACITY;
        
        pthread_mutex_lock(&ht->entries[index].mutex);
        struct list_entry *le = NULL;
        SLIST_FOREACH(le, &ht->entries[index].list_head, pointers) {
            if (strcmp(le->key, key) == 0) {
                le->value = value;
                goto found;
            }
        }
        
        le = malloc(sizeof(*le));
        le->key = key;  // Transfer ownership
        le->value = value;
        SLIST_INSERT_HEAD(&ht->entries[index].list_head, le, pointers);
        
    found:
        pthread_mutex_unlock(&ht->entries[index].mutex);
    }
    cache->count = 0;
    cache->dirty = false;
    pthread_mutex_unlock(&cache->lock);
}

static void flush_all_caches(struct hash_table_v2 *ht) {
    pthread_mutex_lock(&master_lock);
    struct thread_cache *cache;
    SLIST_FOREACH(cache, &master_list, pointers) {
        flush_thread_cache(ht, cache);
    }
    pthread_mutex_unlock(&master_lock);
}

// --- Public API ---
struct hash_table_v2 *hash_table_v2_create() {
    struct hash_table_v2 *ht = calloc(1, sizeof(*ht));
    assert(ht != NULL);
    
    for (size_t i = 0; i < HASH_TABLE_CAPACITY; i++) {
        SLIST_INIT(&ht->entries[i].list_head);
        pthread_mutex_init(&ht->entries[i].mutex, NULL);
    }
    
    // Register main thread's cache
    register_thread_cache();
    return ht;
}

void hash_table_v2_add_entry(struct hash_table_v2 *ht, 
                            const char *key, 
                            uint32_t value) {
    // Register cache on first use
    static _Thread_local bool registered = false;
    if (!registered) {
        register_thread_cache();
        registered = true;
    }
    
    pthread_mutex_lock(&thread_cache.lock);
    
    // Flush if full
    if (thread_cache.count == CACHE_SIZE) {
        flush_thread_cache(ht, &thread_cache);
    }
    
    // Add to cache
    thread_cache.entries[thread_cache.count++] = (struct cache_entry){
        .key = strdup(key),
        .value = value
    };
    thread_cache.dirty = true;
    
    pthread_mutex_unlock(&thread_cache.lock);
}

bool hash_table_v2_contains(struct hash_table_v2 *ht, const char *key) {
    flush_all_caches();  // Ensure all caches are flushed
    
    uint32_t index = bernstein_hash(key) % HASH_TABLE_CAPACITY;
    pthread_mutex_lock(&ht->entries[index].mutex);
    
    bool found = false;
    struct list_entry *le;
    SLIST_FOREACH(le, &ht->entries[index].list_head, pointers) {
        if (strcmp(le->key, key) == 0) {
            found = true;
            break;
        }
    }
    
    pthread_mutex_unlock(&ht->entries[index].mutex);
    return found;
}

void hash_table_v2_destroy(struct hash_table_v2 *ht) {
    flush_all_caches(ht);  // Final flush
    
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
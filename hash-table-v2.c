#include "hash-table-base.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <pthread.h>

#define CACHE_SIZE 4

struct cache_entry {
    char *key;
    uint32_t value;
};

struct thread_cache {
    struct cache_entry entries[CACHE_SIZE];
    size_t count;
};

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

static _Thread_local struct thread_cache thread_cache = { .count = 0 };

static void flush_cache(struct hash_table_v2 *hash_table)
{
    for (size_t i = 0; i < thread_cache.count; i++) {
        struct cache_entry *cache_entry = &thread_cache.entries[i];
        uint32_t index = bernstein_hash(cache_entry->key) % HASH_TABLE_CAPACITY;
        struct hash_table_entry *hash_entry = &hash_table->entries[index];
        
        pthread_mutex_lock(&hash_entry->mutex);
        
        struct list_entry *list_entry = NULL;
        SLIST_FOREACH(list_entry, &hash_entry->list_head, pointers) {
            if (strcmp(list_entry->key, cache_entry->key) == 0) {
                list_entry->value = cache_entry->value;
                goto found;
            }
        }
        
        // Not found, create new entry
        list_entry = malloc(sizeof(struct list_entry));
        assert(list_entry != NULL);
        list_entry->key = cache_entry->key; // Transfer ownership
        list_entry->value = cache_entry->value;
        SLIST_INSERT_HEAD(&hash_entry->list_head, list_entry, pointers);
        
    found:
        pthread_mutex_unlock(&hash_entry->mutex);
        
        // Mark as flushed
        cache_entry->key = NULL;
    }
    
    thread_cache.count = 0;
}

struct hash_table_v2 *hash_table_v2_create()
{
    struct hash_table_v2 *hash_table = calloc(1, sizeof(struct hash_table_v2));
    assert(hash_table != NULL);
    
    for (size_t i = 0; i < HASH_TABLE_CAPACITY; i++) {
        struct hash_table_entry *entry = &hash_table->entries[i];
        SLIST_INIT(&entry->list_head);
        int rc = pthread_mutex_init(&entry->mutex, NULL);
        assert(rc == 0);
    }
    
    return hash_table;
}

void hash_table_v2_add_entry(struct hash_table_v2 *hash_table,
                           const char *key,
                           uint32_t value)
{
    // If cache is full, flush it
    if (thread_cache.count == CACHE_SIZE) {
        flush_cache(hash_table);
    }
    
    // Add to cache (make a copy of the key)
    thread_cache.entries[thread_cache.count].key = strdup(key);
    assert(thread_cache.entries[thread_cache.count].key != NULL);
    thread_cache.entries[thread_cache.count].value = value;
    thread_cache.count++;
}

bool hash_table_v2_contains(struct hash_table_v2 *hash_table,
                          const char *key)
{
    // First flush our cache to ensure consistency
    flush_cache(hash_table);
    
    uint32_t index = bernstein_hash(key) % HASH_TABLE_CAPACITY;
    struct hash_table_entry *entry = &hash_table->entries[index];
    
    pthread_mutex_lock(&entry->mutex);
    
    bool found = false;
    struct list_entry *list_entry;
    SLIST_FOREACH(list_entry, &entry->list_head, pointers) {
        if (strcmp(list_entry->key, key) == 0) {
            found = true;
            break;
        }
    }
    
    pthread_mutex_unlock(&entry->mutex);
    return found;
}

uint32_t hash_table_v2_get_value(struct hash_table_v2 *hash_table,
                                const char *key)
{
    // First flush our cache to ensure we get the latest value
    flush_cache(hash_table);
    
    uint32_t index = bernstein_hash(key) % HASH_TABLE_CAPACITY;
    struct hash_table_entry *entry = &hash_table->entries[index];
    
    pthread_mutex_lock(&entry->mutex);
    
    struct list_entry *list_entry;
    SLIST_FOREACH(list_entry, &entry->list_head, pointers) {
        if (strcmp(list_entry->key, key) == 0) {
            uint32_t value = list_entry->value;
            pthread_mutex_unlock(&entry->mutex);
            return value;
        }
    }
    
    pthread_mutex_unlock(&entry->mutex);
    assert(false && "Key not found");
    return 0;
}

void hash_table_v2_destroy(struct hash_table_v2 *hash_table)
{
    // Flush any remaining entries in our thread's cache
    flush_cache(hash_table);
    
    for (size_t i = 0; i < HASH_TABLE_CAPACITY; i++) {
        struct hash_table_entry *entry = &hash_table->entries[i];
        
        // No need to lock since we're destroying
        struct list_entry *list_entry;
        while (!SLIST_EMPTY(&entry->list_head)) {
            list_entry = SLIST_FIRST(&entry->list_head);
            SLIST_REMOVE_HEAD(&entry->list_head, pointers);
            free((void*)list_entry->key);
            free(list_entry);
        }
        
        pthread_mutex_destroy(&entry->mutex);
    }
    
    free(hash_table);
}
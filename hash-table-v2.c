#include "hash-table-base.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <pthread.h>

// Thread-local storage for cache
#define CACHE_SIZE 16

struct cache_entry {
    char *key; // Deep copy of the key
    uint32_t value;
};

struct thread_cache {
    struct cache_entry entries[CACHE_SIZE];
    size_t count; // Number of entries in the cache
};

struct list_entry {
    const char *key;
    uint32_t value;
    SLIST_ENTRY(list_entry) pointers;
};

SLIST_HEAD(list_head, list_entry);

struct hash_table_entry {
    struct list_head list_head;
    pthread_mutex_t *mutex;
};

struct hash_table_v2 {
    struct hash_table_entry entries[HASH_TABLE_CAPACITY];
};

// Thread-local cache (no synchronization needed, complies with mutex-only spec)
static _Thread_local struct thread_cache thread_cache = { .count = 0 };

struct hash_table_v2 *hash_table_v2_create()
{
    struct hash_table_v2 *hash_table = calloc(1, sizeof(struct hash_table_v2));
    assert(hash_table != NULL);
    for (size_t i = 0; i < HASH_TABLE_CAPACITY; ++i) {
        struct hash_table_entry *entry = &hash_table->entries[i];
        SLIST_INIT(&entry->list_head);
        entry->mutex = malloc(sizeof(pthread_mutex_t));
        assert(entry->mutex != NULL);
        int error = pthread_mutex_init(entry->mutex, NULL);
        if (error != 0) {
            for (size_t j = 0; j < i; ++j) {
                pthread_mutex_destroy(hash_table->entries[j].mutex);
                free(hash_table->entries[j].mutex);
            }
            free(hash_table);
            exit(error);
        }
    }
    return hash_table;
}

static struct hash_table_entry *get_hash_table_entry(struct hash_table_v2 *hash_table,
                                                    const char *key)
{
    assert(key != NULL);
    uint32_t index = bernstein_hash(key) % HASH_TABLE_CAPACITY;
    return &hash_table->entries[index];
}

static struct list_entry *get_list_entry(struct hash_table_v2 *hash_table,
                                        const char *key,
                                        struct list_head *list_head)
{
    assert(key != NULL);
    struct list_entry *entry = NULL;
    SLIST_FOREACH(entry, list_head, pointers) {
        if (strcmp(entry->key, key) == 0) {
            return entry;
        }
    }
    return NULL;
}

// Flush the entire thread-local cache to the hash table
static void flush_cache(struct hash_table_v2 *hash_table)
{
    if (thread_cache.count == 0) {
        return;
    }

    // Process each cache entry
    for (size_t i = 0; i < thread_cache.count; ++i) {
        uint32_t bucket = bernstein_hash(thread_cache.entries[i].key) % HASH_TABLE_CAPACITY;
        struct hash_table_entry *hash_table_entry = &hash_table->entries[bucket];
        struct list_head *list_head = &hash_table_entry->list_head;

        int error = pthread_mutex_lock(hash_table_entry->mutex);
        if (error != 0) {
            exit(error);
        }

        struct list_entry *list_entry = get_list_entry(hash_table, thread_cache.entries[i].key, list_head);
        if (list_entry != NULL) {
            list_entry->value = thread_cache.entries[i].value;
        } else {
            list_entry = calloc(1, sizeof(struct list_entry));
            list_entry->key = thread_cache.entries[i].key; // Transfer ownership
            list_entry->value = thread_cache.entries[i].value;
            SLIST_INSERT_HEAD(list_head, list_entry, pointers);
        }

        error = pthread_mutex_unlock(hash_table_entry->mutex);
        if (error != 0) {
            exit(error);
        }

        thread_cache.entries[i].key = NULL; // Key ownership transferred or updated
    }

    // Clear the cache
    for (size_t i = 0; i < thread_cache.count; ++i) {
        if (thread_cache.entries[i].key != NULL) {
            free(thread_cache.entries[i].key); // Free any untransferred keys
        }
    }
    thread_cache.count = 0;
}

bool hash_table_v2_contains(struct hash_table_v2 *hash_table, const char *key)
{
    // Flush the entire cache to ensure all entries are in the hash table
    if (thread_cache.count > 0) {
        flush_cache(hash_table);
    }

    struct hash_table_entry *hash_table_entry = get_hash_table_entry(hash_table, key);
    struct list_head *list_head = &hash_table_entry->list_head;
    int error = pthread_mutex_lock(hash_table_entry->mutex);
    if (error != 0) {
        exit(error);
    }

    struct list_entry *list_entry = get_list_entry(hash_table, key, list_head);
    error = pthread_mutex_unlock(hash_table_entry->mutex);
    if (error != 0) {
        exit(error);
    }

    return list_entry != NULL;
}

void hash_table_v2_add_entry(struct hash_table_v2 *hash_table, const char *key, uint32_t value)
{
    // If cache is full, flush it
    if (thread_cache.count == CACHE_SIZE) {
        flush_cache(hash_table);
    }

    // Add to cache
    thread_cache.entries[thread_cache.count].key = strdup(key); // Deep copy
    assert(thread_cache.entries[thread_cache.count].key != NULL);
    thread_cache.entries[thread_cache.count].value = value;
    thread_cache.count++;
}

uint32_t hash_table_v2_get_value(struct hash_table_v2 *hash_table, const char *key)
{
    // Flush the entire cache to ensure all entries are in the hash table
    if (thread_cache.count > 0) {
        flush_cache(hash_table);
    }

    struct hash_table_entry *hash_table_entry = get_hash_table_entry(hash_table, key);
    struct list_head *list_head = &hash_table_entry->list_head;
    int error = pthread_mutex_lock(hash_table_entry->mutex);
    if (error != 0) {
        exit(error);
    }

    struct list_entry *list_entry = get_list_entry(hash_table, key, list_head);
    assert(list_entry != NULL);
    uint32_t value = list_entry->value;
    error = pthread_mutex_unlock(hash_table_entry->mutex);
    if (error != 0) {
        exit(error);
    }

    return value;
}

void hash_table_v2_destroy(struct hash_table_v2 *hash_table)
{
    // Flush any remaining cache entries
    if (thread_cache.count > 0) {
        flush_cache(hash_table);
    }

    for (size_t i = 0; i < HASH_TABLE_CAPACITY; ++i) {
        struct hash_table_entry *entry = &hash_table->entries[i];
        struct list_head *list_head = &entry->list_head;
        struct list_entry *list_entry = NULL;
        while (!SLIST_EMPTY(list_head)) {
            list_entry = SLIST_FIRST(list_head);
            SLIST_REMOVE_HEAD(list_head, pointers);
            free((char *)list_entry->key); // Free the key (deep copied)
            free(list_entry);
        }
        int error = pthread_mutex_destroy(entry->mutex);
        if (error != 0) {
            exit(error);
        }
        free(entry->mutex);
    }
    free(hash_table);
}
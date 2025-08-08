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

static void flush_cache(struct hash_table_v2 *hash_table);

struct hash_table_v2 *hash_table_v2_create()
{
    struct hash_table_v2 *hash_table = calloc(1, sizeof(struct hash_table_v2));
    assert(hash_table != NULL);
    for (size_t i = 0; i < HASH_TABLE_CAPACITY; ++i) {
        struct hash_table_entry *entry = &hash_table->entries[i];
        SLIST_INIT(&entry->list_head);
        int error = pthread_mutex_init(&entry->mutex, NULL);
        if (error != 0) {
            for (size_t j = 0; j < i; ++j) {
                pthread_mutex_destroy(&hash_table->entries[j].mutex);
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

static void flush_cache(struct hash_table_v2 *hash_table)
{
    if (thread_cache.count == 0) {
        return;
    }

    for (size_t i = 0; i < thread_cache.count; ++i) {
        struct cache_entry *cache_entry = &thread_cache.entries[i];
        struct hash_table_entry *hash_table_entry = 
            get_hash_table_entry(hash_table, cache_entry->key);
        
        int error = pthread_mutex_lock(&hash_table_entry->mutex);
        if (error != 0) {
            exit(error);
        }

        struct list_entry *list_entry = get_list_entry(
            hash_table, 
            cache_entry->key, 
            &hash_table_entry->list_head
        );

        if (list_entry != NULL) {
            list_entry->value = cache_entry->value;
        } else {
            list_entry = calloc(1, sizeof(struct list_entry));
            list_entry->key = cache_entry->key; // Transfer ownership
            list_entry->value = cache_entry->value;
            SLIST_INSERT_HEAD(&hash_table_entry->list_head, list_entry, pointers);
        }

        error = pthread_mutex_unlock(&hash_table_entry->mutex);
        if (error != 0) {
            exit(error);
        }

        // Mark as transferred (don't free since we transferred ownership)
        cache_entry->key = NULL;
    }

    thread_cache.count = 0;
}

bool hash_table_v2_contains(struct hash_table_v2 *hash_table, const char *key)
{
    flush_cache(hash_table);

    struct hash_table_entry *hash_table_entry = get_hash_table_entry(hash_table, key);
    int error = pthread_mutex_lock(&hash_table_entry->mutex);
    if (error != 0) {
        exit(error);
    }

    struct list_entry *list_entry = get_list_entry(
        hash_table, 
        key, 
        &hash_table_entry->list_head
    );

    error = pthread_mutex_unlock(&hash_table_entry->mutex);
    if (error != 0) {
        exit(error);
    }

    return list_entry != NULL;
}

void hash_table_v2_add_entry(struct hash_table_v2 *hash_table, const char *key, uint32_t value)
{
    if (thread_cache.count == CACHE_SIZE) {
        flush_cache(hash_table);
    }

    // Add to cache
    thread_cache.entries[thread_cache.count].key = strdup(key);
    assert(thread_cache.entries[thread_cache.count].key != NULL);
    thread_cache.entries[thread_cache.count].value = value;
    thread_cache.count++;
}

uint32_t hash_table_v2_get_value(struct hash_table_v2 *hash_table, const char *key)
{
    flush_cache(hash_table);

    struct hash_table_entry *hash_table_entry = get_hash_table_entry(hash_table, key);
    int error = pthread_mutex_lock(&hash_table_entry->mutex);
    if (error != 0) {
        exit(error);
    }

    struct list_entry *list_entry = get_list_entry(
        hash_table, 
        key, 
        &hash_table_entry->list_head
    );
    assert(list_entry != NULL);
    uint32_t value = list_entry->value;

    error = pthread_mutex_unlock(&hash_table_entry->mutex);
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
            free((char *)list_entry->key);
            free(list_entry);
        }
        pthread_mutex_destroy(&entry->mutex);
    }
    free(hash_table);
}
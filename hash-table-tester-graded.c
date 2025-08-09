#include "hash-table-base.h"
#include "hash-table-v1.h"
#include "hash-table-v2.h"

#include <argp.h>
#include <limits.h>
#include <locale.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#define BYTES_PER_STRING 8
#define NUM_RUNS 3

struct trial_result {
	int trial_num;
	bool correctness_ok;
	unsigned long best_base_time;
	unsigned long best_v1_time;
	unsigned long best_v2_time;
	char *v1_perf_result;
	char *v2_perf_result;
	bool trial_passed;
	double high_perf_target;
	double weak_perf_target;
};

struct arguments {
	uint32_t threads;
	uint32_t size;
	uint32_t trials;
};

static struct argp_option options[] = {
	{ "threads", 't', "NUM", 0, "Number of threads." },
	{ "size", 's', "NUM", 0, "Size per thread." },
	{ "trials", 'r', "NUM", 0, "Number of trials to run." },
	{ 0 }
};

static uint32_t parse_uint32_t(const char *string)
{
	uint32_t current = 0;
	uint8_t i = 0;
	while (true) {
		char c = string[i];
		if (c == 0) {
			break;
		}
		if (i == 10) {
			exit(EINVAL);
		}
		if (c < 0x30 || c > 0x39) {
			exit(EINVAL);
		}
		uint8_t digit = (c - 0x30);
		if (i == 9) {
			if (current > 429496729) {
				exit(EINVAL);
			} else if (current == 429496729 && digit > 5) {
				exit(EINVAL);
			}
		}
		current = current * 10 + digit;
		++i;
	}
	return current;
}

static error_t parse_opt(int key, char *arg, struct argp_state *state)
{
	struct arguments *arguments = state->input;
	switch (key) {
	case 't':
		arguments->threads = parse_uint32_t(arg);
		break;
	case 's':
		arguments->size = parse_uint32_t(arg);
		break;
	case 'r':
		arguments->trials = parse_uint32_t(arg);
		break;
	}
	return 0;
}

static struct arguments arguments;
static char *data;

static size_t get_global_index(uint32_t thread, uint32_t index)
{
	return thread * arguments.size + index;
}

static char *get_string(size_t global_index)
{
	return data + (global_index * BYTES_PER_STRING);
}

static unsigned long usec_diff(struct timeval *a, struct timeval *b)
{
	unsigned long usec;
	usec = (b->tv_sec - a->tv_sec) * 1000000;
	usec += b->tv_usec - a->tv_usec;
	return usec;
}

static struct hash_table_v1 *hash_table_v1;

void *run_v1(void *arg)
{
	uint32_t thread = (uintptr_t)arg;
	for (uint32_t j = 0; j < arguments.size; ++j) {
		size_t global_index = get_global_index(thread, j);
		char *string = get_string(global_index);
		hash_table_v1_add_entry(hash_table_v1, string, global_index);
	}
	return NULL;
}

static struct hash_table_v2 *hash_table_v2;

void *run_v2(void *arg)
{
	uint32_t thread = (uintptr_t)arg;
	for (uint32_t j = 0; j < arguments.size; ++j) {
		size_t global_index = get_global_index(thread, j);
		char *string = get_string(global_index);
		hash_table_v2_add_entry(hash_table_v2, string, global_index);
	}
	return NULL;
}

int main(int argc, char *argv[])
{
	arguments.threads = 4;
	arguments.size = 25000;
	arguments.trials = 1;

	static struct argp argp = { options, parse_opt };
	argp_parse(&argp, argc, argv, 0, 0, &arguments);

	setlocale(LC_ALL, "en_US.UTF-8");

	data = calloc(arguments.threads * arguments.size, BYTES_PER_STRING);
	if (!data) {
		perror("Failed to allocate data");
		return 1;
	}

	struct timeval start, end;
	gettimeofday(&start, NULL);
	srand(42);
	for (uint32_t i = 0; i < arguments.threads; ++i) {
		for (uint32_t j = 0; j < arguments.size; ++j) {
			size_t global_index = get_global_index(i, j);
			char *string = get_string(global_index);
			for (uint32_t k = 0; k < (BYTES_PER_STRING - 1); ++k) {
				int r = rand() % 52;
				string[k] = (r < 26) ? (r + 0x41) : (r + 0x47);
			}
			string[BYTES_PER_STRING - 1] = 0;
		}
	}
	gettimeofday(&end, NULL);
	printf("Data generation: %'lu usec\n", usec_diff(&start, &end));
	printf("Configuration: %u threads, %'u size per thread, %u trials\n",
	       arguments.threads, arguments.size, arguments.trials);

	pthread_t *threads = calloc(arguments.threads, sizeof(pthread_t));
	if (!threads) {
		perror("Failed to allocate threads");
		free(data);
		return 1;
	}

	struct trial_result *results =
		calloc(arguments.trials, sizeof(struct trial_result));
	if (!results) {
		perror("Failed to allocate results");
		free(threads);
		free(data);
		return 1;
	}

	for (uint32_t trial = 0; trial < arguments.trials; ++trial) {
		printf("\n--- Starting Trial %u/%u ---\n", trial + 1,
		       arguments.trials);
		results[trial].trial_num = trial + 1;

		unsigned long base_timings[NUM_RUNS], v1_timings[NUM_RUNS],
			v2_timings[NUM_RUNS];
		size_t base_missing[NUM_RUNS], v1_missing[NUM_RUNS],
			v2_missing[NUM_RUNS];

		for (int run = 0; run < NUM_RUNS; ++run) {
			printf("  - Run %d/%d:\n", run + 1, NUM_RUNS);

			struct hash_table_base *hash_table_base =
				hash_table_base_create();
			printf("    - Running base... ");
			gettimeofday(&start, NULL);
			for (uint32_t i = 0; i < arguments.threads; ++i) {
				for (uint32_t j = 0; j < arguments.size; ++j) {
					size_t global_index =
						get_global_index(i, j);
					char *string = get_string(global_index);
					hash_table_base_add_entry(
						hash_table_base, string,
						global_index);
				}
			}
			gettimeofday(&end, NULL);
			base_timings[run] = usec_diff(&start, &end);
			base_missing[run] = 0;
			for (uint32_t i = 0; i < arguments.threads; ++i) {
				for (uint32_t j = 0; j < arguments.size; ++j) {
					size_t global_index =
						get_global_index(i, j);
					char *string = get_string(global_index);
					if (!hash_table_base_contains(
						    hash_table_base, string)) {
						base_missing[run]++;
					}
				}
			}
			if (base_missing[run] > 0) {
				printf("FAILED (%'zu missing)\n",
				       base_missing[run]);
			} else {
				printf("PASSED\n");
			}
			hash_table_base_destroy(hash_table_base);

			hash_table_v1 = hash_table_v1_create();
			printf("    - Running v1... ");
			gettimeofday(&start, NULL);
			for (uintptr_t i = 0; i < arguments.threads; ++i) {
				pthread_create(&threads[i], NULL, run_v1,
					       (void *)i);
			}
			for (uintptr_t i = 0; i < arguments.threads; ++i) {
				pthread_join(threads[i], NULL);
			}
			gettimeofday(&end, NULL);
			v1_timings[run] = usec_diff(&start, &end);
			v1_missing[run] = 0;
			for (uint32_t i = 0; i < arguments.threads; ++i) {
				for (uint32_t j = 0; j < arguments.size; ++j) {
					size_t global_index =
						get_global_index(i, j);
					char *string = get_string(global_index);
					if (!hash_table_v1_contains(
						    hash_table_v1, string)) {
						v1_missing[run]++;
					}
				}
			}
			if (v1_missing[run] > 0) {
				printf("FAILED (%'zu missing)\n",
				       v1_missing[run]);
			} else {
				printf("PASSED\n");
			}
			hash_table_v1_destroy(hash_table_v1);

			hash_table_v2 = hash_table_v2_create();
			printf("    - Running v2... ");
			gettimeofday(&start, NULL);
			for (uintptr_t i = 0; i < arguments.threads; ++i) {
				pthread_create(&threads[i], NULL, run_v2,
					       (void *)i);
			}
			for (uintptr_t i = 0; i < arguments.threads; ++i) {
				pthread_join(threads[i], NULL);
			}
			gettimeofday(&end, NULL);
			v2_timings[run] = usec_diff(&start, &end);
			v2_missing[run] = 0;
			for (uint32_t i = 0; i < arguments.threads; ++i) {
				for (uint32_t j = 0; j < arguments.size; ++j) {
					size_t global_index =
						get_global_index(i, j);
					char *string = get_string(global_index);
					if (!hash_table_v2_contains(
						    hash_table_v2, string)) {
						v2_missing[run]++;
					}
				}
			}
			if (v2_missing[run] > 0) {
				printf("FAILED (%'zu missing)\n",
				       v2_missing[run]);
			} else {
				printf("PASSED\n");
			}
			hash_table_v2_destroy(hash_table_v2);
		}

		results[trial].correctness_ok = true;
		for (int i = 0; i < NUM_RUNS; ++i) {
			if (base_missing[i] > 0 || v1_missing[i] > 0 ||
			    v2_missing[i] > 0) {
				results[trial].correctness_ok = false;
				break;
			}
		}

		unsigned long best_base_time = ULONG_MAX,
			      best_v1_time = ULONG_MAX,
			      best_v2_time = ULONG_MAX;
		for (int i = 0; i < NUM_RUNS; ++i) {
			if (base_timings[i] < best_base_time)
				best_base_time = base_timings[i];
			if (v1_timings[i] < best_v1_time)
				best_v1_time = v1_timings[i];
			if (v2_timings[i] < best_v2_time)
				best_v2_time = v2_timings[i];
		}
		results[trial].best_base_time = best_base_time;
		results[trial].best_v1_time = best_v1_time;
		results[trial].best_v2_time = best_v2_time;

		bool v1_passed = false;
		if (best_v1_time > best_base_time) {
			results[trial].v1_perf_result = strdup("PASSED");
			v1_passed = true;
		} else {
			results[trial].v1_perf_result = strdup("FAILED");
		}

		long num_cores = sysconf(_SC_NPROCESSORS_ONLN);
		if (num_cores <= 1)
			num_cores = 2;
		results[trial].high_perf_target =
			(double)best_base_time / (num_cores - 1);
		results[trial].weak_perf_target =
			(double)best_base_time / (num_cores / 2.0);

		bool v2_passed = false;
		if (best_v2_time <= results[trial].high_perf_target) {
			results[trial].v2_perf_result =
				strdup("PASSED (High Perf)");
			v2_passed = true;
		} else if (best_v2_time <= results[trial].weak_perf_target) {
			results[trial].v2_perf_result =
				strdup("PASSED (Weak Perf)");
			v2_passed = true;
		} else {
			results[trial].v2_perf_result = strdup("FAILED");
		}

		results[trial].trial_passed =
			results[trial].correctness_ok && v1_passed && v2_passed;
	}

	printf("\n\n===================================================================== TRIAL SUMMARY =====================================================================\n");
	printf("| %-5s | %-11s | %-14s | %-14s | %-10s | %-14s | %-19s | %-19s | %-20s |\n",
	       "Trial", "Correctness", "Base Time (us)", "V1 Time (us)",
	       "V1 Result", "V2 Time (us)", "V2 High Target (us)",
	       "V2 Weak Target (us)", "V2 Result");
	printf("---------------------------------------------------------------------------------------------------------------------------------------------------------\n");

	int trials_passed_count = 0;
	for (uint32_t i = 0; i < arguments.trials; ++i) {
		printf("| #%-4d | %-11s | %'14lu | %'14lu | %-10s | %'14lu | %'18.0f | %'18.0f | %-20s |\n",
		       results[i].trial_num,
		       results[i].correctness_ok ? "PASSED" : "FAILED",
		       results[i].best_base_time, results[i].best_v1_time,
		       results[i].v1_perf_result, results[i].best_v2_time,
		       results[i].high_perf_target,
		       results[i].weak_perf_target, results[i].v2_perf_result);
		if (results[i].trial_passed) {
			trials_passed_count++;
		}
	}
	printf("---------------------------------------------------------------------------------------------------------------------------------------------------------\n");

	printf("\nOverall Result: %d/%d trials passed.\n", trials_passed_count,
	       arguments.trials);

	for (uint32_t i = 0; i < arguments.trials; ++i) {
		free(results[i].v1_perf_result);
		free(results[i].v2_perf_result);
	}
	free(results);
	free(threads);
	free(data);

	return 0;
}
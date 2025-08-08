#Custom python script to test for detailed rubric guidelines.

import re
import subprocess
import unittest

class TestLab4(unittest.TestCase):

    def _make():
        result = subprocess.run(['make'], capture_output=True, text=True)
        return result

    def _make_clean():
        result = subprocess.run(['make', 'clean'], capture_output=True, text=True)
        return result

    @classmethod
    def setUpClass(cls):
        cls.make = cls._make().returncode == 0

    @classmethod
    def tearDownClass(cls):
        cls._make_clean()

    def _run_test(self, threads, entries):
        print(f"Running tester with -t {threads} -s {entries}...")
        self.assertTrue(self.make, msg='make failed')
        hash_result = subprocess.check_output(['./hash-table-tester', '-t', str(threads), '-s', str(entries)]).decode()
        nums = re.sub(r'Generation: ([\d\,]+) usec\nHash table base: ([\d\,]+) usec\n  - ([\d\,]+) missing\nHash table v1: ([\d\,]+) usec\n  - ([\d\,]+) missing\nHash table v2: ([\d\,]+) usec\n  - ([\d\,]+) missing\n',
                      r'\1|\2|\3|\4|\5|\6|\7',
                      hash_result)
        gen, base, miss_0, v1, miss_1, v2, miss_2 = nums.split('|')
        return {
            'gen': int(gen.replace(",", "")),
            'base': int(base.replace(",", "")),
            'miss_0': int(miss_0.replace(",", "")),
            'v1': int(v1.replace(",", "")),
            'miss_1': int(miss_1.replace(",", "")),
            'v2': int(v2.replace(",", "")),
            'miss_2': int(miss_2.replace(",", ""))
        }

    def test_v1_performance(self):
        results = [self._run_test(4, 50000) for _ in range(3)]
        for i, result in enumerate(results, 1):
            print(f"\nRun {i}:")
            print(f"Average Times over 30 runs: Base={result['base']:,} usec, V1={result['v1']:,} usec")
            print(f"V1 Performance Grade (based on average) ---")
            self.assertEqual(result['miss_1'], 0, msg=f"Run {i}: V1 missing entries should be 0 but got {result['miss_1']}")
            self.assertGreater(result['v1'], result['base'], msg=f"Run {i}: V1 must be slower than Base but got V1={result['v1']:,} usec vs Base={result['base']:,} usec")
            print(f"PASSED: V1 was slower than Base, as expected.")
        print(f"Result: PASSED Test 1 (High Performance Criteria Met)")

    def test_v2_performance(self):
        results = [self._run_test(4, 50000) for _ in range(3)]
        cores = 4
        for i, result in enumerate(results, 1):
            print(f"\nRun {i}:")
            print(f"Average Times over 30 runs: Base={result['base']:,} usec, V2={result['v2']:,} usec")
            print(f"V2 Performance Grade (based on average) ---")
            self.assertEqual(result['miss_2'], 0, msg=f"Run {i}: V2 missing entries should be 0 but got {result['miss_2']}")
            high_target = result['base'] / (cores - 1)
            weak_target = result['base'] / (cores / 2)
            if result['v2'] <= high_target:
                print(f"Number of cores detected: {cores}")
                print(f"High performance target (Test 1: base / (cores - 1)): <= {high_target:,.0f} usec")
                print(f"Weak performance target (Test 2: base / (cores / 2)): <= {weak_target:,.0f} usec")
                print(f"Result: PASSED Test 1 (High Performance Criteria Met)")
            elif result['v2'] <= weak_target:
                print(f"Number of cores detected: {cores}")
                print(f"High performance target (Test 1: base / (cores - 1)): <= {high_target:,.0f} usec")
                print(f"Weak performance target (Test 2: base / (cores / 2)): <= {weak_target:,.0f} usec")
                print(f"Result: PASSED Test 2 (High Performance Criteria Met)")
            else:
                print(f"Number of cores detected: {cores}")
                print(f"High performance target (Test 1: base / (cores - 1)): <= {high_target:,.0f} usec")
                print(f"Weak performance target (Test 2: base / (cores / 2)): <= {weak_target:,.0f} usec")
                print(f"Result: PASSED Test 3 (Low Performance Criteria Met)")

if __name__ == '__main__':
    unittest.main()
#!/bin/bash
# Usage:
#   ./run_tests.sh [TOTAL_runs] [TestName1 [TestName2 ...]]
# Examples:
#   ./run_tests.sh                     # run all tests 100 times
#   ./run_tests.sh 50                  # run all tests 50 times
#   ./run_tests.sh 30 Test1            # run Test1 (and its subtests) 30 times
#   ./run_tests.sh 20 TestPut TestGet  # run TestPut & TestGet 20 times

PASSED=0
FAILED=0

TOTAL="${1:-100}" # Default to 100 runs if not provided
if ! [[ "$TOTAL" =~ ^[0-9]+$ ]]; then
  TOTAL=100
  NAMES=("$@")
else
  shift || true
  NAMES=("$@")
fi

# Build regex pattern for test name filters
if [ "${#NAMES[@]}" -eq 0 ]; then
  TESTS="."
else
  parts=()
  for name in "${NAMES[@]}"; do
    parts+=("(${name})(/|$)")
  done
  TESTS="^($(IFS='|'; echo "${parts[*]}"))"
fi

# Get the full list of matching tests once
ALL_TESTS=$(go test -list . | grep -E "$TESTS" | grep -v '^ok' | grep -v 'no test files' || true)

if [ -z "$ALL_TESTS" ]; then
  echo "No matching tests found."
  exit 1
fi

echo "Running the following tests individually (each with 2m timeout):"
echo "$ALL_TESTS"
echo ""

for ((run=1; run<=TOTAL; run++)); do
  echo "========== RUN $run/$TOTAL =========="
  for test_name in $ALL_TESTS; do
    echo -n "Running $test_name ... "
    LOGFILE="test_output_${run}_${test_name}.log"

    if go test -run "^${test_name}$" -timeout 2m -count=1 -v > "$LOGFILE" 2>&1; then
      echo "PASSED"
      ((PASSED++))
      #rm "$LOGFILE"
    else
      echo "FAILED"
      ((FAILED++))
    fi
  done
done

echo ""
echo "===== Test Results Summary ====="
TOTAL_TESTS=$((TOTAL * $(echo "$ALL_TESTS" | wc -w)))
echo "Total test executions: $TOTAL_TESTS"
echo "Passed: $PASSED"
echo "Failed: $FAILED"
if [ "$TOTAL_TESTS" -gt 0 ]; then
  echo "Success rate: $(( PASSED * 100 / TOTAL_TESTS ))%"
fi

if [ $FAILED -gt 0 ]; then
  echo ""
  echo "Failed test logs:"
  ls -la test_output_*.log 2>/dev/null
else
  echo ""
  echo "All tests passed"
fi

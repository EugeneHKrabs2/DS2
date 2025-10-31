#!/bin/bash


testSuite=("TestBasic" , "TestDeaf" , "TestOld" , "TestManyForget" , "TestManyUnreliable" , "TestPartition" , "TestForget" , "TestLots" , "TestMany" , "TestForgetMem" , "TestRPCCount")


# If you want to run the entire test suite, uncomment the line above and comment the suite defined below


# place the specific test you want to run here as a comma-separated list
#testSuite=("TestLots")


# configurable progress interval
progress_interval=10


# WARNING: 
#
# Num_procs is a per-test parameter of processes. If you run 4 tests, this script will spawn 8 processes!!!! 
#
# This is okay for most personal laptops these days, but may starve threads on a 2 core VM.
#
total_runs=500
num_procs=2




# Run one test up to N times in its own process, stop on first failure
run_test_loop() {
    local testname=$1
    local start=$2
    local end=$3
    local outfile="output_${testname}_${start}_${end}.txt"
    rm -f "$outfile"

    for ((i=start; i<=end; i++)); do
        echo "Run #$i for $testname" >> "$outfile"
        go test -run="^$testname\$" -count=1 -timeout=120s ./... >> "$outfile" 2>&1
        result=$?
        echo "----------------------------------------" >> "$outfile"

        if (( result != 0 )); then
            echo "[FAIL-FAST] $testname failed on run #$i"
            kill $PPID   # kill parent script if any sub-process fails
            exit 1
        fi

        if (( (i - start + 1) % progress_interval == 0 )); then
            echo "[Progress] $testname: $((i - start + 1)) runs completed in this worker"
        fi
    done
}

# Spawn multiple processes per test
for t in "${testSuite[@]}"; do
    chunk=$((total_runs / num_procs))
    for ((p=0; p<num_procs; p++)); do
        start=$((p * chunk + 1))
        end=$(( (p+1) * chunk ))
        if (( p == num_procs - 1 )); then
            end=$total_runs  # last worker takes remainder
        fi
        run_test_loop "$t" "$start" "$end" &
    done
done

# Wait for all processes to finish
wait

echo "All test loops completed."
#!/bin/bash
# benchmark.sh - Simple benchmark utility
# Usage: ./benchmark.sh <command>

ITERATIONS="${2:-10}"
COMMAND="$1"

if [ -z "$COMMAND" ]; then
    echo "Usage: $0 <command> [iterations]"
    echo "Example: $0 'python script.py' 10"
    exit 1
fi

echo "=== Benchmark ==="
echo "Command: $COMMAND"
echo "Iterations: $ITERATIONS"
echo ""

TIMES=()

for i in $(seq 1 $ITERATIONS); do
    START=$(date +%s.%N)
    eval "$COMMAND" > /dev/null 2>&1
    END=$(date +%s.%N)
    ELAPSED=$(echo "$END - $START" | bc)
    TIMES+=($ELAPSED)
    printf "Run %d: %.3fs\n" $i $ELAPSED
done

# Calculate statistics
echo ""
echo "## Results"

TOTAL=0
for t in "${TIMES[@]}"; do
    TOTAL=$(echo "$TOTAL + $t" | bc)
done

AVG=$(echo "scale=3; $TOTAL / $ITERATIONS" | bc)
MIN=$(printf '%s\n' "${TIMES[@]}" | sort -n | head -1)
MAX=$(printf '%s\n' "${TIMES[@]}" | sort -n | tail -1)

echo "| Metric | Value |"
echo "|--------|-------|"
echo "| Average | ${AVG}s |"
echo "| Min | ${MIN}s |"
echo "| Max | ${MAX}s |"
echo "| Total | ${TOTAL}s |"

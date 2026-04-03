for item in "$@"
do
  python3 automation/run_part2a.py --workload="$item"
  sleep 10
done

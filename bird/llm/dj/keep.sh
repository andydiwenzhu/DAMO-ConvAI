while true; do
python dj/test.py --limit 800 --results_filename new_results/results_with_knowledge_fewshots.csv --use_knowledge --use_fewshots
python dj/utils.py --results_filename new_results/results_with_knowledge_fewshots.csv
sleep 10
done

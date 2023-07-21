while true; do
python dj/sp_test.py --results_filename vicuna_results/results_with_knowledge.csv --limit 400 --use_knowledge
python dj/utils.py --results_filename vicuna_results/results_with_knowledge.csv
sleep 10
done

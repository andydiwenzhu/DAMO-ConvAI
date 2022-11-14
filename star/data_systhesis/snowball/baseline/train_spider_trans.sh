CUDA_VISIBLE_DEVICES=1 fairseq-train data-bin/spider.sql-en \
    --optimizer adam --lr 0.25 --clip-norm 0.1 --dropout 0.2 --max-tokens 4000 \
    --arch transformer --save-dir checkpoints/spider_trans --save-interval 100 --max-epoch 1000

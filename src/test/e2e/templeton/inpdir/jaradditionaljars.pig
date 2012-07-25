-- no register command
l = load '$INPDIR/nums.txt' as (a : chararray);
f = foreach l generate  org.apache.pig.piggybank.evaluation.string.Reverse($0);
store f into '$OUTDIR/reversed.txt';


register 'udfs.py' using jython as myfuncs;
l = load '$INPDIR/nums.txt' as (a : chararray);
f = foreach l generate $0, myfuncs.helloworld() ;
store f into '$OUTDIR/pyhello.txt';


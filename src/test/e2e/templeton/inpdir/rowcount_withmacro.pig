import 'rowcountmacro.pig';

l = load '$INPDIR/nums.txt';
c = row_count(l);
store c into '$OUTDIR/rowcount.out';

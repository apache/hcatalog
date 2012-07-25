DEFINE row_count(X) RETURNS Z { Y = group $X all; $Z = foreach Y generate COUNT($X); };

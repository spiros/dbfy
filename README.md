dbfy
====

dbfy will transform a collection of flat CSV files into a functional SQLIte 
database than can then be queried using traditional SQL leading to higher productivity (maybe) and
increased profits (probably not).

1. Load a collection of CSV files into a SQLite database
2. ???
3. Profit!

```bash

perl dbfy 
  --output output.sqlite 
	--input demographics.csv  
	--input mri_ct.csv 
	--input minap.csv 
	--input mi_cardiothoracic.csv 
	--input dev_angio.csv 
```

will result in a SQLite database with the contents of the five files in respective tables.

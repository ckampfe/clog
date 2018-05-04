# clog

Query CSVs with datalog

## Installation

`$ boot build-native-image`

## Usage

```
xcxk066$> wc -l junk.csv
   10001 junk.csv
xcxk066$> head -10 junk.csv
id,name,kind
1,bill,kind
2,sally,sad
3,ernie,eerie
4,jill,jolly
5,bill,kind
6,sally,sad
7,ernie,eerie
8,jill,jolly
9,bill,kind
xcxk066$> ./clog junk.csv -i -t "{:id :to-int}"
xcxk066$> time ./clog junk.csv -q '[:find ?name (count ?e) :where [?e :name ?name]]' -t '{:id :to-long}'
(["jill" 2500] ["ernie" 2500] ["sally" 2500] ["bill" 2500])
./clog junk.csv -q '[:find ?name (count ?e) :where [?e :name ?name]]' -t   0.14s user 0.03s system 98% cpu 0.175 total
```

## License

Copyright © 2018 Clark Kampfe

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

# Macbook 2019:

```
 $ cargo run --release ~/.config/
 ...
 Timing...
      rayon: 388.412175ms
  rayon p2s: 323.274985ms
   parallel: 363.826755ms
       sync: 569.793907ms
 {"parallel": 21297628, "rayon p2s": 22212186, "syn": 21300624, "rayon": 22329026}
```

# Linux box - 24 cores, 112GB Mem
```
 $ cargo run --release /some/dir/with/lots/of/javascript/files
Timing...
     rayon: 20.466664974s
 rayon p2s: 14.468034459s
  parallel: 21.965607378s
      sync: 18.48489502s
{"syn": 960857322, "rayon": 975555982, "rayon p2s": 995650276, "parallel": 965608098}
```

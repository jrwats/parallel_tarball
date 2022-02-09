# Macbook 2019:

```
$ cargo run --release ~/.config/ -b 64 -i 10
warning: version requirement `0.10.0+zstd.1.5.2` for dependency `zstd` includes semver metadata which will be ignored, removing the metadata is recommended to avoid confusion
   Compiling par_tar v0.1.0 (/Users/jwatson/workspace/parallel_tarball)
    Finished release [optimized] target(s) in 4.46s
     Running `target/release/par_tar /Users/jwatson/.config/ -b 64 -i 10`
Args { dir: "/Users/jwatson/.config/", methods: None, iterations: Some(10), buffer_size: Some(64) }
RayonParallelThenSync 3.291949071s
ParallelTokioSpawn 3.793436146s
RayonHashingReader 3.869304644s
RayonAll       3.939261549s
Serial         5.210947879s
ParallelHashingReader 5.267120999s
SerialHashingReader 5.908980717s
{
    "Serial": 21300624,
    "RayonHashingReader": 22833930,
    "RayonAll": 22279291,
    "ParallelTokioSpawn": 21273027,
    "ParallelHashingReader": 21310335,
    "SerialHashingReader": 21300624,
    "RayonParallelThenSync": 21815328,
}
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

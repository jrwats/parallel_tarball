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
$ cargo run --release ~/.config -i 3 -b 64
...
RayonParallelThenSync 3.244664736s
SerialHashingReader 3.549346524s
RayonHashingReader 3.574306796s
Serial         3.607791001s
ParallelHashingReader 3.997673462s
ParallelTokioSpawn 4.034907148s
RayonAll       4.075306344s
{
    "RayonAll": 44980431,
    "ParallelHashingReader": 43841072,
    "RayonHashingReader": 46307969,
    "RayonParallelThenSync": 44906472,
    "SerialHashingReader": 43705638,
    "Serial": 43705638,
    "ParallelTokioSpawn": 43991400,
}
```

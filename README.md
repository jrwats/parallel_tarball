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
$ cargo run --release ~/.config/ -b 64 -i 10
RayonParallelThenSync 69.377131867s
RayonHashingReader 69.422753029s
Serial         77.332775152s
SerialHashingReader 80.217449183s
RayonAll       86.131195054s
ParallelTokioSpawn 86.521539267s
ParallelHashingReader 89.276931043s
key mismatch
key mismatch
{
    "RayonHashingReader": 45944708,
    "ParallelTokioSpawn": 44001781,
    "Serial": 43705638,
    "RayonParallelThenSync": 45349697,
    "RayonAll": 44532780,
    "ParallelHashingReader": 43880730,
    "SerialHashingReader": 43705638,
}
```

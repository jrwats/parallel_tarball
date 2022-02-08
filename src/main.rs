use anyhow::{anyhow, Context};
use core::time::Duration;
use derive_more::{Display};
use futures::{
    stream, Future, StreamExt, TryStreamExt,
    future::BoxFuture
};
use maplit::hashmap;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, IoSliceMut, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::AsyncReadExt;
use walkdir::WalkDir;

struct HashingReader {
    file: File,
    hasher: blake3::Hasher,
    len: u64,
}

impl Read for HashingReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.file.read(buf)?; // we're losing time here
        self.hasher.update(&buf[0..n]);
        Ok(n)
    }
}

impl HashingReader {
    fn try_new(file: File) -> anyhow::Result<Self> {
        let metadata = file.metadata().context("reading metadata")?;
        let len = metadata.len();
        let hasher = blake3::Hasher::new();
        Ok(Self { file, hasher, len })
    }

    fn len(&self) -> u64 {
        self.len
    }

    #[inline]
    fn finalize(&self) -> String {
        self.hasher.finalize().to_string()
    }
}

/*
struct MmapHashingReader {
    file: File,
    hasher: blake3::Hasher,
    len: u64,
}

impl MMapHashingReader {
    fn try_new(file: File) -> anyhow::Result<Self> {
        let metadata = file.metadata().context("reading metadata")?;
        let len = metadata.len();
        let hasher = blake3::Hasher::new();
        Ok(Self { file, hasher, len })
    }

    fn len(&self) -> u64 {
        self.len
    }

    #[inline]
    fn finalize(&self) -> String {
        self.hasher.finalize().to_string()
    }
}
*/

// impl std::io::SizeHint for HashingReader {
//     #[inline]
//     fn lower_bound(&self) -> usize {
//         0
//     }
//
//     #[inline]
//     fn upper_bound(&self) -> Option<usize> {
//         self.len
//     }
// }

fn naive_open<P: AsRef<Path>>(path: P) -> anyhow::Result<Option<File>> {
    match File::open(path.as_ref()) {
        Ok(f) => Ok(Some(f)),
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => Ok(None),
            _kind => Ok(None), // eat the error
        },
    }
}

fn maybe_open<P: AsRef<Path>>(path: P) -> anyhow::Result<Option<File>> {
    match File::open(path.as_ref()) {
        Ok(f) => Ok(Some(f)),
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => Ok(None),
            _kind => Err(anyhow!(e)),
        },
    }
}

fn blake_hash(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn get_bytes(file: &mut File) -> anyhow::Result<Vec<u8>> {
    let metadata = file.metadata().context("reading metadata")?;
    let mut buf = Vec::with_capacity(metadata.len() as usize);
    file.seek(SeekFrom::Start(0)).context("seeking to start")?;
    file.read_to_end(&mut buf).context("reading")?;
    Ok(buf)
}

fn append_path_data<W: Write>(
    tar: &mut tar::Builder<W>,
    path: &Path,
    buf: &[u8],
) -> anyhow::Result<()> {
    append_with_len(tar, path, buf, buf.len() as u64)
}

fn append_with_len<W: Write, R: Read>(
    tar: &mut tar::Builder<W>,
    path: &Path,
    data: R,
    len: u64,
) -> anyhow::Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(len);
    tar.append_data(&mut header, &path, data)
        .with_context(|| format!("appending {:#?}", path))?;
    Ok(())
}

async fn locked_hashed_read<W: Write>(
    tar_lock: Arc<Mutex<tar::Builder<W>>>,
    path: PathBuf,
    dir: PathBuf,
) -> anyhow::Result<String> {
    let mut tar = tar_lock.lock().unwrap();
    sync_hashed_read(&mut tar, &path, &dir)
}


const BUF_SIZE: usize = 128 * 1024;

fn sync_hashed_read<W: Write>(
    tar: &mut tar::Builder<W>,
    path: &Path,
    dir: &Path,
) -> anyhow::Result<String> {
    if let Some(file) = naive_open(dir.join(path))? {
        let hashed_reader = HashingReader::try_new(file)?;
        // let mut bufread = std::io::BufReader::new(hashed_reader);
        let mut bufread = std::io::BufReader::with_capacity(BUF_SIZE, hashed_reader);
        let len = bufread.get_ref().len();
        append_with_len(tar, path, &mut bufread, len)
            .with_context(|| format!("reading {:#?}", path))?;
        return Ok(bufread.get_ref().finalize());
    }
    Ok("".into())
}

async fn sync_add_to_tar<W: Write>(
    tar_lock: Arc<Mutex<tar::Builder<W>>>,
    path: PathBuf,
    dir: PathBuf,
) -> anyhow::Result<String> {
    if let Some(mut file) = naive_open(dir.join(&path))? {
        let buf = get_bytes(&mut file)?;
        let mut tar = tar_lock.lock().unwrap();
        append_path_data(&mut tar, &path, &buf)?;
        return Ok(blake_hash(&buf));
    }
    Ok("".into())
}

// This async file I/O is FAR slower than sync I/O
async fn _async_add_to_tar<W: Write>(
    tar_lock: Arc<Mutex<tar::Builder<W>>>,
    path: PathBuf,
    dir: PathBuf,
) -> anyhow::Result<usize> {
    let read_path = dir.join(&path);
    match tokio::fs::File::open(&read_path).await {
        Ok(mut file) => {
            let metadata = file.metadata().await?;
            let mut buf = Vec::with_capacity(metadata.len() as usize);
            file.read_to_end(&mut buf).await?;
            let mut header = tar::Header::new_gnu();
            header.set_size(buf.len() as u64);
            let mut tar = tar_lock.lock().unwrap();
            tar.append_data(&mut header, &path, buf.as_slice())
                .with_context(|| format!("appending {:#?}", &path))?;
            Ok(buf.len())
        }
        _ => Ok(0),
    }
}

struct Buf(Arc<Mutex<std::io::Cursor<Vec<u8>>>>);

impl Write for Buf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut cursor = self.0.lock().unwrap();
        cursor.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut cursor = self.0.lock().unwrap();
        cursor.flush()
    }
}

async fn spawned_hash<W: 'static + Write + Send>(
    tar_lock: Arc<Mutex<tar::Builder<W>>>,
    path: &Path,
    dir: &Path,
) -> anyhow::Result<(PathBuf, String)> {
    let p = path.to_owned();
    let d = dir.to_owned();
    let hash = tokio::spawn(sync_add_to_tar(tar_lock.clone(), p.clone(), d))
        .await
        .unwrap()?;
    Ok((p, hash))
}

async fn spawned_hashed_reader<W: 'static + Write + Send>(
    tar_lock: Arc<Mutex<tar::Builder<W>>>,
    path: &Path,
    dir: &Path,
) -> anyhow::Result<(PathBuf, String)> {
    let p = path.to_owned();
    let d = dir.to_owned();
    let hash = tokio::spawn(locked_hashed_read(tar_lock.clone(), p.clone(), d))
        .await
        .unwrap()?;
    Ok((p, hash))
}

async fn par_stream_tar(
    dir: &Path,
    paths: &[PathBuf],
) -> anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)> {
    let buf = Arc::new(Mutex::new(std::io::Cursor::new(vec![])));
    let checksums = {
        let encoder = zstd::stream::write::Encoder::new(Buf(buf.clone()), 0)
            .context("setting up zstd encoder")?
            .auto_finish();
        let tar_lock = Arc::new(Mutex::new(tar::Builder::new(encoder)));

        let sums: HashMap<PathBuf, String> = stream::iter(paths)
            .map(|path| async { spawned_hash(tar_lock.clone(), path, dir).await })
            .buffer_unordered(2000)
            .try_collect::<Vec<(PathBuf, String)>>()
            .await
            .context("writing tar file")?
            .into_iter()
            .collect();
        let mut tar = tar_lock.lock().unwrap();
        tar.finish().context("writing tarball")?;
        sums
    };
    let lock = Arc::try_unwrap(buf).expect("Lock still has multiple owners");
    let cursor = lock.into_inner().expect("Mutex can't be locked");
    Ok((cursor.into_inner(), checksums))
}

async fn par_hashed_reader(
    dir: &Path,
    paths: &[PathBuf],
) -> anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)> {
    let buf = Arc::new(Mutex::new(std::io::Cursor::new(vec![])));
    let checksums = {
        let encoder = zstd::stream::write::Encoder::new(Buf(buf.clone()), 0)
            .context("setting up zstd encoder")?
            .auto_finish();
        let tar_lock = Arc::new(Mutex::new(tar::Builder::new(encoder)));

        let sums: HashMap<PathBuf, String> = stream::iter(paths)
            .map(|path| async { spawned_hashed_reader(tar_lock.clone(), path, dir).await })
            .buffer_unordered(2000)
            .try_collect::<Vec<(PathBuf, String)>>()
            .await
            .context("writing tar file")?
            .into_iter()
            .collect();
        let mut tar = tar_lock.lock().unwrap();
        tar.finish().context("writing tarball")?;
        sums
    };
    let lock = Arc::try_unwrap(buf).expect("Lock still has multiple owners");
    let cursor = lock.into_inner().expect("Mutex can't be locked");
    Ok((cursor.into_inner(), checksums))
}

// async not really needed ...
fn rayon_tar(dir: &Path, paths: &[PathBuf]) -> anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)> {
    let mut bytes: Vec<u8> = Vec::new();
    let checksums = {
        let encoder = zstd::stream::write::Encoder::new(&mut bytes, 0)
            .context("setting up zstd encoder")?
            .auto_finish();
        let tar_lock = Arc::new(Mutex::new(tar::Builder::new(encoder)));
        let checksums: HashMap<PathBuf, String> = paths
            .par_iter()
            .map(|path| -> anyhow::Result<Option<(PathBuf, String)>> {
                if let Some(mut file) = naive_open(PathBuf::from(dir).join(path))? {
                    let buf = get_bytes(&mut file)?;
                    let mut tar = tar_lock.lock().unwrap();
                    append_path_data(&mut tar, &path, &buf)?;
                    let hash = blake_hash(&buf);
                    return Ok(Some((PathBuf::from(path), hash)));
                }
                Ok(None)
            })
            .collect::<anyhow::Result<Vec<Option<(PathBuf, String)>>>>()
            .context("writing tar file")?
            .into_iter()
            .flatten()
            .collect();
        let mut tar = tar_lock.lock().unwrap();
        tar.finish().context("writing tarball")?;
        checksums
    };
    Ok((bytes, checksums))
}

async fn rayon_par2sync(
    dir: &Path,
    paths: &[PathBuf],
) -> anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)> {
    let handle = {
        let (send, recv): (std::sync::mpsc::SyncSender<(PathBuf, Vec<u8>, String)>, _) =
            std::sync::mpsc::sync_channel(rayon::current_num_threads());
        let handle = tokio::spawn(async move {
            let mut bytes: Vec<u8> = vec![];
            let checksums = {
                let encoder = zstd::stream::write::Encoder::new(&mut bytes, 0)
                    .context("setting up zstd encoder")?
                    .auto_finish();
                let mut tar = tar::Builder::new(encoder);

                let mut sums = HashMap::new();
                for (p, file_bytes, hash) in recv.into_iter() {
                    append_path_data(&mut tar, &p, &file_bytes)?;
                    sums.insert(p.to_owned(), hash);
                }
                tar.finish().context("writing tarball")?;
                sums
            };
            Ok::<(Vec<u8>, HashMap<PathBuf, String>), anyhow::Error>((bytes, checksums))
        });

        let read_errs: Vec<_> = paths
            .par_iter()
            .map(|path: &PathBuf| -> anyhow::Result<()> {
                if let Some(mut file) = maybe_open(PathBuf::from(dir).join(path))? {
                    let buf: Vec<u8> =
                        get_bytes(&mut file).with_context(|| format!("reading {:#?}", path))?;
                    let hash = blake_hash(&buf);
                    send.send((path.clone(), buf, hash))?;
                }
                Ok(())
            })
            .filter(Result::is_err)
            .collect();

        for e in read_errs {
            eprintln!("{:?}", e);
        }
        handle
    };
    let res = handle.await?;
    Ok(res?)
}

// rayon on file open only. HashingReader later
async fn rayon_open<P: AsRef<Path> + Sync>(
    dir: P,
    paths: &[PathBuf],
) -> anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)> {
    let result = {
        let (send, recv): (std::sync::mpsc::SyncSender<(PathBuf, HashingReader)>, _) =
            std::sync::mpsc::sync_channel(rayon::current_num_threads());
        let handle = tokio::spawn(async move {
            let mut bytes: Vec<u8> = vec![];
            let checksums = {
                let encoder = zstd::stream::write::Encoder::new(&mut bytes, 0)
                    .context("setting up zstd encoder")?
                    .auto_finish();
                let mut tar = tar::Builder::new(encoder);

                let mut checksums = HashMap::new();
                for (path, mut hasher) in recv.into_iter() {
                    let len = hasher.len();
                    append_with_len(&mut tar, &path, &mut hasher, len)
                        .with_context(|| format!("Writing {:#?} into tarball", &path))?;
                    checksums.insert(path, hasher.finalize());
                }
                tar.finish().context("writing tarball")?;
                checksums
            };
            Ok::<(Vec<u8>, HashMap<PathBuf, String>), anyhow::Error>((bytes, checksums))
        });

        let _: Vec<_> = paths
            .par_iter()
            .map(|path: &PathBuf| -> anyhow::Result<()> {
                let open_result = maybe_open(dir.as_ref().join(&path));
                if let Ok(Some(file)) = open_result {
                    let reader = HashingReader::try_new(file)?;
                    send.send((path.to_owned(), reader))?;
                } else if let Err(e) = open_result {
                    // It's possible we encounter a file we can't read on disk, like a socket.
                    // Don't fail a `dotsync push` for this.  Log it:
                    // TODO: surface these to user somehow?
                    eprintln!("{:?}", e);
                }
                Ok(())
            })
            .collect();
        handle
    }
    .await
    .context("tokio join handle failure")?;
    Ok(result?)
}

fn serial_tar(dir: &Path, paths: &[PathBuf]) -> anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)> {
    let mut bytes: Vec<u8> = Vec::new();
    let checksums = {
        let encoder =
            zstd::stream::write::Encoder::new(&mut bytes, 0).context("setting up zstd encoder")?;
        let mut tar = tar::Builder::new(encoder);
        let mut checksums = HashMap::new();
        paths.iter().for_each(|path| {
            let hash = sync_hashed_read(&mut tar, path, dir).expect("hashing broke");
            checksums.insert(PathBuf::from(path), hash);
        });
        tar.finish().context("writing tarball")?;
        checksums
    };
    Ok((bytes, checksums))
}

fn serial_hr(dir: &Path, paths: &[PathBuf]) -> anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)> {
    let mut bytes: Vec<u8> = Vec::new();
    let checksums = {
        let encoder =
            zstd::stream::write::Encoder::new(&mut bytes, 0).context("setting up zstd encoder")?;
        let mut tar = tar::Builder::new(encoder);
        let mut checksums = HashMap::new();
        paths
            .iter()
            .map(|path| -> anyhow::Result<()> {
                let hash = sync_hashed_read(&mut tar, path, dir).context("read/hash")?;
                checksums.insert(path.to_owned(), hash);
                Ok(())
            })
            .collect::<anyhow::Result<Vec<()>>>()
            .context("writing tar file")?;
        tar.finish().context("writing tarball")?;
        checksums
    };
    Ok((bytes, checksums))
}

// Helper panics on assert, but not before ensuring we spew a useful error
fn rel_path<'a, 'b>(child: &'a Path, parent: &'b Path) -> &'a Path {
    let res = child.strip_prefix(parent);
    if res.is_err() {
        eprintln!("Expected {:#?} to be descendant of {:#?}", child, parent);
    }
    res.unwrap()
}

const ITER: i32 = 1;

// type Tarballer = dyn Fn(&Path, &[PathBuf]) -> Box<dyn Future<Output = anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)>>>;
// type TBall = dyn Fn(&Path, &[PathBuf]) -> BoxFuture<'static, anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)>>;

#[derive(Clone, Copy, Debug, Display)]
enum Run {
    RayonAll,
    RayonParallelThenSync,
    RayonHashingReader,
    ParallelTokioSpawn,
    ParallelHashingReader,
    Serial,
    SerialHashingReader,
}

#[inline]
async fn run(
    run_type: Run,
    dir: &Path,
    paths: &[PathBuf]
) -> anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)> {
    match run_type {
        Run::RayonAll => rayon_tar(dir, paths),
        Run::RayonParallelThenSync => rayon_par2sync(dir, paths).await,
        Run::RayonHashingReader => rayon_open(dir, paths).await,
        Run::ParallelTokioSpawn => par_stream_tar(dir, paths).await,
        Run::ParallelHashingReader => par_hashed_reader(dir, paths).await,
        Run::Serial => serial_tar(dir, paths),
        Run::SerialHashingReader => serial_hr(dir, paths),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Timing...");

    let args: Vec<String> = std::env::args().collect();
    let dir = PathBuf::from(&args[1]);

    let paths: Vec<PathBuf> = WalkDir::new(&dir)
        .into_iter()
        .filter_map(|r| {
            r.ok()
                .and_then(|e| (!e.path().is_dir()).then(|| rel_path(e.path(), &dir).to_owned()))
        })
        .collect();

    // dummy call to warm up the disk
    let (_, _) = rayon_par2sync(&dir, &paths).await?;

    let mut times: HashMap<String, Duration> = HashMap::new();
    let mut bs: HashMap<String, usize> = HashMap::new();
    let mut csums: HashMap<String, HashMap<PathBuf, String>> = HashMap::new();

    let to_run = vec![
        Run::ParallelHashingReader,
        Run::ParallelTokioSpawn,
        // Run::RayonAll,
        // Run::RayonHashingReader,
        // Run::RayonParallelThenSync,
        // Run::Serial,
        // Run::SerialHashingReader,
    ];
    for e in to_run.iter() {
        let e_str = e.to_string();
        let start = Instant::now();
        let mut run_csums: HashMap<PathBuf, String> = HashMap::new();
        for _i in 0..ITER {
            let (bytes, r_csums) = run(*e, &dir, &paths).await?;
            run_csums = r_csums;
        }
        let duration = start.elapsed();
        csums.insert(e_str, run_csums);
        times.insert(e.to_string(), duration);
    }

    let mut times: Vec<(String, Duration)> = times.into_iter().collect();
    times.sort_by_cached_key(|t| t.1);
    for (lbl, dur) in times {
        println!("{:14} {:?}", lbl, dur);
    }
    // println!("{:?}", times);
    // println!("  rayon p2s: {:?}", rayon_s_elapsed);
    // println!("      rayon: {:?}", rayon_elapsed);
    // println!("parallel hr: {:?}", par_hr_elapsed);
    // println!("   parallel: {:?}", par_elapsed);
    // println!("   rayon hr: {:?}", rayon_o_elapsed);
    // println!("       sync: {:?}", sync_elapsed);

    let mut iter = csums.iter();
    let first = iter.next().unwrap();
    for sums in iter {
        if first.1 != sums.1 {
            eprintln!("{:?} {:?} don't match", first.0, sums.0);
        }
    }

    println!("{:?}", bs);
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::iter;

    #[test]
    fn equivalent() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let tmpdir = PathBuf::from(dir.path());
        let mut foo = File::create(tmpdir.join("foo.txt"))?;
        let strs: Vec<String> = iter::repeat(String::from("abcdefghijklmnopqrstuvwxyz"))
            .take(100)
            .collect();
        let content = strs.join("_");
        foo.write_all(content.as_bytes())?;
        let files = vec![PathBuf::from("foo.txt")];
        let par_tarball = rayon_tar(&tmpdir, &files)?;
        let serial_tarball = serial_tar(&tmpdir, &files)?;
        assert_eq!(par_tarball, serial_tarball);
        Ok(())
    }
}

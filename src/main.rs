use anyhow::{anyhow, Context};
use core::time::Duration;
use derive_more::{Display};
use futures::{
    stream, StreamExt, TryStreamExt,
};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::str::FromStr;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc::{self, Sender, SyncSender}};
use std::time::Instant;
use structopt::StructOpt;
use strum_macros::EnumString;
use tokio::io::AsyncReadExt;
use walkdir::WalkDir;

#[derive(StructOpt, Debug)]
struct Args {
    /// Directory to scan
    dir: String,

    /// Limit the runner methods to these type(s) (useful for strace)
    #[structopt(short, long)]
    methods: Option<Vec<String>>,

    /// number of iterations to perform
    #[structopt(short, long)]
    iterations: Option<i32>,

    /// Read buffer size (in KB) for the BufReader wrapper around HashingReader
    /// i.e. RayonAll, RayonParallelThenSync, RayonHashingReader, ParallelTokioSpawn,
    ///      ParallelHashingReader, Serial, SerialHashingReader,
    #[structopt(short, long)]
    buffer_size: Option<usize>,
}

struct HashingReader {
    file: File,
    hasher: blake3::Hasher,
}

impl Read for HashingReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.file.read(buf)?;
        self.hasher.update(&buf[0..n]);
        Ok(n)
    }
}

impl HashingReader {
    fn try_new(file: File) -> anyhow::Result<Self> {
        let hasher = blake3::Hasher::new();
        Ok(Self { file, hasher })
    }

    #[inline]
    fn finalize(&self) -> String {
        self.hasher.finalize().to_string()
    }
}

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
) -> anyhow::Result<Option<String>> {
    let mut tar = tar_lock.lock().unwrap();
    sync_hashed_read(&mut tar, &path, &dir)
}

static mut READ_BUF: usize = 256 * 1024; // 256KB

unsafe fn set_buf_size(kb: usize) {
    READ_BUF = kb * 1024;
}

fn sync_hashed_read<W: Write>(
    tar: &mut tar::Builder<W>,
    path: &Path,
    dir: &Path,
) -> anyhow::Result<Option<String>> {
    if let Some(file) = naive_open(dir.join(path))? {
        let metadata = file.metadata().context("reading metadata")?;
        let len = metadata.len() as u64;
        let hashed_reader = HashingReader::try_new(file)?;
        let buf = unsafe { READ_BUF };
        let mut bufread = BufReader::with_capacity(buf, hashed_reader);
        append_with_len(tar, path, &mut bufread, len)
            .with_context(|| format!("reading {:#?}", path))?;
        return Ok(Some(bufread.get_ref().finalize()));
    }
    Ok(None)
}

async fn sync_add_to_tar<W: Write>(
    tar_lock: Arc<Mutex<tar::Builder<W>>>,
    path: PathBuf,
    dir: PathBuf,
) -> anyhow::Result<Option<String>> {
    if let Some(mut file) = naive_open(dir.join(&path))? {
        let buf = get_bytes(&mut file)?;
        let mut tar = tar_lock.lock().unwrap();
        append_path_data(&mut tar, &path, &buf)?;
        return Ok(Some(blake_hash(&buf)));
    }
    Ok(None)
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
) -> anyhow::Result<Option<(PathBuf, String)>> {
    let p = path.to_owned();
    let d = dir.to_owned();
    let maybe_hash = tokio::spawn(sync_add_to_tar(tar_lock.clone(), p.clone(), d))
        .await
        .unwrap()?;
    if let Some(hash) = maybe_hash {
        return Ok(Some((p, hash)))
    }
    Ok(None)
}

async fn spawned_hashed_reader<W: 'static + Write + Send>(
    tar_lock: Arc<Mutex<tar::Builder<W>>>,
    path: &Path,
    dir: &Path,
) -> anyhow::Result<Option<(PathBuf, String)>> {
    let p = path.to_owned();
    let d = dir.to_owned();
    let maybe_hash = tokio::spawn(locked_hashed_read(tar_lock.clone(), p.clone(), d))
        .await
        .unwrap()?;
    if let Some(hash) = maybe_hash {
        return Ok(Some((p, hash)))
    }
    Ok(None)
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
            .try_collect::<Vec<Option<(PathBuf, String)>>>()
            .await
            .context("writing tar file")?
            .into_iter()
            .filter_map(|opt| opt)
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
            .try_collect::<Vec<Option<(PathBuf, String)>>>()
            .await
            .context("writing tar file")?
            .into_iter()
            .filter_map(|opt| opt)
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
        let (send, recv): (SyncSender<(PathBuf, Vec<u8>, String)>, _) =
            mpsc::sync_channel(rayon::current_num_threads());
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

struct TarPacket {
    path: PathBuf,
    bytes: Vec<u8>,
    final_hash: Option<String>,
}

#[inline]
async fn rayon_hr<P: AsRef<Path> + Sync>(
    dir: P,
    paths: &[PathBuf],
) -> anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)> {
    let result = {
        let (send, recv): (SyncSender<TarPacket>, _) =
            std::sync::mpsc::sync_channel(rayon::current_num_threads());
        let handle = tokio::spawn(async move {
            let mut bytes: Vec<u8> = vec![];
            let checksums = {
                let encoder = zstd::stream::write::Encoder::new(&mut bytes, 0)
                    .context("setting up zstd encoder")?
                    .auto_finish();
                let mut tar = tar::Builder::new(encoder);

                let mut checksums = HashMap::new();
                for packet in recv.into_iter() {
                    let len = packet.bytes.len();
                    append_with_len(&mut tar, &packet.path, packet.bytes.as_slice(), len as u64)
                        .with_context(|| format!("Writing {:#?} into tarball", &packet.path))?;
                    if let Some(hash) = packet.final_hash {
                        checksums.insert(packet.path, hash);
                    }
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
                    let buf_size = unsafe { READ_BUF };
                    let mut reader = HashingReader::try_new(file)?;
                    loop {
                        let mut v = vec![0; buf_size];
                        let len = reader.read(v.as_mut())?;
                        unsafe { v.set_len(len); }
                        let final_hash = if len < buf_size {
                            Some(reader.finalize())
                        } else { 
                            None
                        };

                        let packet = TarPacket {
                            path: path.to_owned(),
                            bytes: v,
                            final_hash,
                        };
                        send.send(packet)?;
                        if len < buf_size {
                            break;
                        }
                    }
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

#[inline]
async fn rayon_hr_ac<P: AsRef<Path> + Sync>(
    dir: P,
    paths: &[PathBuf],
) -> anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)> {
    let result = {
        let (send, recv): (Sender<TarPacket>, _) = mpsc::channel();
        let handle = tokio::spawn(async move {
            let mut bytes: Vec<u8> = vec![];
            let checksums = {
                let encoder = zstd::stream::write::Encoder::new(&mut bytes, 0)
                    .context("setting up zstd encoder")?
                    .auto_finish();
                let mut tar = tar::Builder::new(encoder);

                let mut checksums = HashMap::new();
                for packet in recv.into_iter() {
                    let len = packet.bytes.len();
                    append_with_len(&mut tar, &packet.path, packet.bytes.as_slice(), len as u64)
                        .with_context(|| format!("Writing {:#?} into tarball", &packet.path))?;
                    if let Some(hash) = packet.final_hash {
                        checksums.insert(packet.path, hash);
                    }
                }
                tar.finish().context("writing tarball")?;
                checksums
            };
            Ok::<(Vec<u8>, HashMap<PathBuf, String>), anyhow::Error>((bytes, checksums))
        });

        let parent = dir.as_ref();
        let path_senders: Vec<(PathBuf, Sender<TarPacket>)> = paths
            .into_iter()
            .map(|p| (p.to_path_buf(), send.clone()))
            .collect();

        let _: Vec<_> = path_senders
            .into_par_iter()
            .map(move |(path, sender)| -> anyhow::Result<()> {
                let open_result = maybe_open(parent.join(&path));
                if let Ok(Some(file)) = open_result {
                    let buf_size = unsafe { READ_BUF };
                    let mut reader = HashingReader::try_new(file)?;
                    loop {
                        let mut v = Vec::with_capacity(buf_size);
                        unsafe { v.set_len(buf_size); }
                        let len = reader.read(v.as_mut())?;
                        unsafe { v.set_len(len); }
                        let final_hash = if len < buf_size {
                            Some(reader.finalize())
                        } else { 
                            None
                        };

                        let packet = TarPacket {
                            path: path.to_owned(),
                            bytes: v,
                            final_hash,
                        };
                        sender.send(packet)?;
                        if len < buf_size {
                            break;
                        }
                    }
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
        paths.iter().map(|path| -> anyhow::Result<()>{
            if let Some(mut file) = naive_open(dir.join(&path))? {
                let buf = get_bytes(&mut file)?;
                append_path_data(&mut tar, &path, &buf)?;
                let hash = blake_hash(&buf);
                checksums.insert(PathBuf::from(path), hash);
            }
            Ok(())
        }).collect::<anyhow::Result<_>>()?;
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
                if let Some(hash) = sync_hashed_read(&mut tar, path, dir).context("read/hash")? {
                    checksums.insert(path.to_owned(), hash);
                }
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

#[derive(Clone, Copy, Debug, Display, EnumString, Eq, Hash, PartialEq, PartialOrd, Ord)]
enum Runner {
    RayonAll,
    RayonParallelThenSync,
    RayonHashingReader,
    RayonHashReadAsyncChan,
    ParallelTokioSpawn,
    ParallelHashingReader,
    Serial,
    SerialHashingReader,
}

#[inline]
async fn run(
    run_type: Runner,
    dir: &Path,
    paths: &[PathBuf]
) -> anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)> {
    match run_type {
        Runner::RayonAll => rayon_tar(dir, paths),
        Runner::RayonParallelThenSync => rayon_par2sync(dir, paths).await,
        Runner::RayonHashingReader => rayon_hr(dir, paths).await,
        Runner::RayonHashReadAsyncChan => rayon_hr_ac(dir, paths).await,
        Runner::ParallelTokioSpawn => par_stream_tar(dir, paths).await,
        Runner::ParallelHashingReader => par_hashed_reader(dir, paths).await,
        Runner::Serial => serial_tar(dir, paths),
        Runner::SerialHashingReader => serial_hr(dir, paths),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut to_run: HashSet<Runner> = HashSet::from([
        Runner::ParallelHashingReader,
        Runner::ParallelTokioSpawn,
        Runner::RayonAll,
        Runner::RayonHashingReader,
        Runner::RayonHashReadAsyncChan,
        Runner::RayonParallelThenSync,
        Runner::Serial,
        Runner::SerialHashingReader,
    ]);

    let args = Args::from_args();
    eprintln!("{:?}", args);

    let dir = PathBuf::from(args.dir);
    if let Some(runners) = args.methods {
        to_run = runners.iter().map(|r| Ok(Runner::from_str(r)?))
            .collect::<anyhow::Result<_>>()?
    }
    let iterations = args.iterations.unwrap_or(ITER);
    if let Some(buffer_size) = args.buffer_size {
        unsafe { set_buf_size(buffer_size) };
    }

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

    for e in to_run.iter() {
        let e_str = e.to_string();
        let mut run_csums: HashMap<PathBuf, String> = HashMap::new();
        let mut run_bytes: usize = 0;
        let start = Instant::now();
        for _i in 0..iterations {
            let (bytes, r_csums) = run(*e, &dir, &paths).await?;
            run_csums = r_csums;
            run_bytes = bytes.len();
        }
        let duration = start.elapsed();
        csums.insert(e_str.clone(), run_csums);
        times.insert(e_str.clone(), duration);
        bs.insert(e_str.clone(), run_bytes);
    }

    let mut times: Vec<(String, Duration)> = times.into_iter().collect();
    times.sort_by_cached_key(|t| t.1);
    for (lbl, dur) in times {
        println!("{:14} {:?}", lbl, dur);
    }

    let mut iter = csums.iter();
    let first = iter.next().unwrap();
    for sums in iter {
        if first.1.len() != sums.1.len() || !first.1.keys().all(|k| sums.1.contains_key(k)) {
            eprintln!("key mismatch");
            eprintln!("{:?}: {:?}, {:?}: {:?}", first.0, first.1.len(), sums.0, sums.1.len());
        }
        for k in first.1.keys() {
            if first.1.get(k) != sums.1.get(k) {
                eprintln!("{:?} => {:?}, {:?} => {:?}", k, first.1.get(k), k, sums.1.get(k));
            }
        }
    }

    println!("{:#?}", bs);
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

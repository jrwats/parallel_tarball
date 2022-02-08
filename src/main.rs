use anyhow::{anyhow, Context};
use futures::{stream, StreamExt, TryStreamExt};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::AsyncReadExt;
use walkdir::WalkDir;

fn naive_open<P: AsRef<Path>>(path: P) -> anyhow::Result<Option<File>> {
    match File::open(path.as_ref()) {
        Ok(f) => Ok(Some(f)),
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => Ok(None),
            _kind => Ok(None) // eat the error
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
    let mut header = tar::Header::new_gnu();
    header.set_size(buf.len() as u64);
    tar.append_data(&mut header, &path, buf)
        .with_context(|| format!("appending {:#?}", &path))?;
    Ok(())
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
        _ => {
            Ok(0)
        }
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
                tar.finish().context("writing tarball")?;

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
                    let buf: Vec<u8> = get_bytes(&mut file)
                        .with_context(|| format!("reading {:#?}", path))?;
                    let hash = blake_hash(&buf);
                    send.send((path.clone(), buf, hash))?;
                }
                Ok(())
            })
            .filter(Result::is_err)
            .collect();

            eprintln!("errs.len(): {}", read_errs.len());
            for e in read_errs {
                eprintln!("{:?}", e);
            }
        handle
    };
    let res = handle.await?;
    Ok(res?)
}

fn sync_tar(dir: &Path, paths: &[PathBuf]) -> anyhow::Result<(Vec<u8>, HashMap<PathBuf, String>)> {
    let mut bytes: Vec<u8> = Vec::new();
    let checksums = {
        let encoder =
            zstd::stream::write::Encoder::new(&mut bytes, 0).context("setting up zstd encoder")?;
        let mut tar = tar::Builder::new(encoder);
        let mut checksums = HashMap::new();
        let _results: Vec<()> = paths
            .iter()
            .map(|path| -> anyhow::Result<()> {
                if let Some(mut file) = naive_open(PathBuf::from(dir).join(path))? {
                    let buf: Vec<u8> = get_bytes(&mut file)?;
                    checksums.insert(PathBuf::from(path), blake_hash(&buf));
                    append_path_data(&mut tar, path, &buf)?;
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

const ITER: i32 = 4;

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

    let mut bs: HashMap<String, usize> = HashMap::new();
    let mut csums: HashMap<String, HashMap<PathBuf, String>> = HashMap::new();
    let rayon_st = Instant::now();
    for _i in 0..ITER {
        let (rbytes, r_csums) = rayon_tar(&dir, &paths)?;
        bs.insert("rayon".into(), rbytes.len());
        csums.insert("rayon".into(), r_csums);
    }
    let rayon_elapsed = rayon_st.elapsed();

    let rayon_s_st = Instant::now();
    for _i in 0..ITER {
        let (rbytes, rp_csums) = rayon_par2sync(&dir, &paths).await?;
        bs.insert("rayon p2s".into(), rbytes.len());
        csums.insert("rayon p2s".into(), rp_csums);
    }
    let rayon_s_elapsed = rayon_s_st.elapsed();

    let parallel_st = Instant::now();
    for _i in 0..ITER {
        let (pbytes, p_csums) = par_stream_tar(&dir, &paths).await?;
        bs.insert("parallel".into(), pbytes.len());
        csums.insert("parallel".into(), p_csums);
    }
    let par_elapsed = parallel_st.elapsed();

    let sync_time = Instant::now();
    for _i in 0..ITER {
        let (sbytes, s_csums) = sync_tar(&dir, &paths)?;
        bs.insert("syn".into(), sbytes.len());
        csums.insert("syn".into(), s_csums);
    }
    let sync_elapsed = sync_time.elapsed();

    println!("     rayon: {:?}", rayon_elapsed);
    println!(" rayon p2s: {:?}", rayon_s_elapsed);
    println!("  parallel: {:?}", par_elapsed);
    println!("      sync: {:?}", sync_elapsed);

    let mut iter = csums.iter();
    let first = iter.next().unwrap();
    for win in iter {
        assert_eq!(first.1, win.1);
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
        let sync_tarball = sync_tar(&tmpdir, &files)?;
        assert_eq!(par_tarball, sync_tarball);
        Ok(())
    }
}

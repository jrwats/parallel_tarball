use anyhow::Context;
use futures::{future, stream, StreamExt, TryStreamExt};
use rayon::prelude::*;
use tokio::io::AsyncReadExt;
use std::fs::File;
use std::io::{Seek, SeekFrom, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use walkdir::WalkDir;

fn maybe_open<P: AsRef<Path>>(path: P) -> anyhow::Result<Option<File>> {
    match File::open(path.as_ref()) {
        Ok(f) => Ok(Some(f)),
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => Ok(None),
            kind => {
                // eprintln!("{:?}", kind);
                // eprintln!("{:?}", e);
                Ok(None)
                // Err(e).context(format!("Failed opening {:#?}", path.as_ref()))
            },
        },
    }
}

async fn add_to_tar<W: Write>(
    tar_lock: Arc<Mutex<tar::Builder<W>>>,
    path: &Path,
    dir: &Path,
) -> anyhow::Result<()> {
    let read_path = PathBuf::from(dir).join(path);
    match tokio::fs::File::open(&read_path).await {
        Ok(mut file) => {
            let mut contents = vec![];
            file.read_buf(&mut contents).await?;
            let mut header = tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            let mut tar = tar_lock.lock().unwrap();
            // eprintln!("appending: {:#?}", path);
            tar.append_data(&mut header, path, contents.as_slice())
                .with_context(|| format!("appending {:#?}", path))?;
            Ok(())
        }
        _ => {
            // eprintln!("Failed reading {:#?}", read_path); 
            Ok(())
        }
    }
}

async fn par_stream_tar(
    dir: &Path,
    paths: &[PathBuf],
) -> anyhow::Result<Vec<u8>> {
    let mut bytes: Vec<u8> = Vec::new();
    {
        let encoder =
            zstd::stream::write::Encoder::new(&mut bytes, 0).context("setting up zstd encoder")?;
        let tar_lock = Arc::new(Mutex::new(tar::Builder::new(encoder)));

        let _results: Vec<()> = stream::iter(paths)
            .map(|path| async { 
                add_to_tar(tar_lock.clone(), path, dir).await
            })
            .buffer_unordered(100)
            .try_collect()
            .await
            .context("writing tar file")?;
        eprintln!("_result len: {}", _results.len());
        let mut tar = tar_lock.lock().unwrap();
        tar.finish().context("writing tarball")?;
    }
    Ok(bytes)
}

fn rayon_tar(
    dir: &Path,
    paths: &[PathBuf],
) -> anyhow::Result<Vec<u8>> {
    let mut bytes: Vec<u8> = Vec::new();
    {
        let encoder =
            zstd::stream::write::Encoder::new(&mut bytes, 0).context("setting up zstd encoder")?;
        let tar_lock = Arc::new(Mutex::new(tar::Builder::new(encoder)));
        let _results: Vec<()> = paths
            .par_iter()
            .map(|path| -> anyhow::Result<()> {
                if let Some(mut file) = maybe_open(PathBuf::from(dir).join(path))? {
                    let metadata = file.metadata().context("reading metadata")?;
                    let mut buf = Vec::with_capacity(metadata.len() as usize);
                    file.seek(SeekFrom::Start(0)).context("seeking to start")?;
                    file.read_to_end(&mut buf).context("reading")?;

                    let mut header = tar::Header::new_gnu();
                    header.set_size(buf.len() as u64);
                    let mut tar = tar_lock.lock().unwrap();
                    tar.append_data(&mut header, path, buf.as_slice())
                        .with_context(|| format!("appending {:#?}", path))?;
                    // let mut tar = tar_lock.lock().unwrap();
                    // tar.append_file(path, &mut file)
                    //     .with_context(|| format!("appending {:#?}", path))?;
                }
                Ok(())
            })
            .collect::<anyhow::Result<Vec<()>>>()
            .context("writing tar file")?;
        let mut tar = tar_lock.lock().unwrap();
        tar.finish().context("writing tarball")?;
    }
    Ok(bytes)
}

fn sync_tar(
    dir: &Path,
    paths: &[PathBuf],
    ) -> anyhow::Result<Vec<u8>> {
    let mut bytes: Vec<u8> = Vec::new();
    {
        let encoder =
            zstd::stream::write::Encoder::new(&mut bytes, 0).context("setting up zstd encoder")?;
        let mut tar = tar::Builder::new(encoder);
        let _results: Vec<()> = paths
            .iter()
            .map(|path| -> anyhow::Result<()> {
                if let Some(mut file) = maybe_open(PathBuf::from(dir).join(path))? {
                    tar.append_file(path, &mut file)
                        .with_context(|| format!("appending {:#?}", path))?;
                }
                Ok(())
            })
        .collect::<anyhow::Result<Vec<()>>>()
            .context("writing tar file")?;
        tar.finish().context("writing tarball")?;
    }
    Ok(bytes)
}


// Helper panics on assert, but not before ensuring we spew a useful error
fn rel_path<'a, 'b>(child: &'a Path, parent: &'b Path) -> &'a Path {
    let res = child.strip_prefix(parent);
    if res.is_err() {
        eprintln!("Expected {:#?} to be descendant of {:#?}", child, parent);
    }
    res.unwrap()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Timing...");

    let args: Vec<String> = std::env::args().collect();
    let dir = PathBuf::from(&args[1]);

    let paths: Vec<PathBuf> = WalkDir::new(&dir)
        .into_iter()
        .filter_map(|r| r.ok().and_then(|e| {
            (!e.path().is_dir()).then(|| rel_path(e.path(), &dir).to_owned())
        }))
        .collect();

    // dummy call to warm up the disk
    let _bytes = sync_tar(&dir, &paths)?;

    let rayon_st = Instant::now();
    let rbytes = rayon_tar(&dir, &paths)?;
    let rayon_elapsed = rayon_st.elapsed();

    let parallel_st = Instant::now();
    let pbytes = par_stream_tar(&dir, &paths).await?;
    let par_elapsed = parallel_st.elapsed();
    
    let sync_time = Instant::now();
    let sbytes = sync_tar(&dir, &paths)?;
    let sync_elapsed = sync_time.elapsed();

    println!("   rayon: {:?}", rayon_elapsed);
    println!("parallel: {:?}", par_elapsed);
    println!("    sync: {:?}", sync_elapsed );

    println!("   rayon bytes: {}", rbytes.len());
    println!("parallel bytes: {}", pbytes.len());
    println!("    sync bytes: {}", sbytes.len());
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
        let strs: Vec<String> = iter::repeat(String::from("abcdefghijklmnopqrstuvwxyz")).take(100).collect();
        let content = strs.join("_");
        foo.write_all(content.as_bytes())?;
        let files = vec![PathBuf::from("foo.txt")];
        let par_tarball = rayon_tar(&tmpdir, &files)?;
        let sync_tarball = sync_tar(&tmpdir, &files)?;
        assert_eq!(par_tarball, sync_tarball);
        Ok(())
    }
}

use std::io;
use std::path::Path;
use std::pin::Pin;

use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use tokio::io::AsyncReadExt;

use crate::core::KvStore;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;

use arrow::array::Array;
use arrow::array::StringArray;

pub struct SimpleFsKvStoreString {
    pub root_dir: String,
    pub max_file_size: u64,
}

pub fn dirents2basenames(
    dirents: tokio::fs::ReadDir,
) -> impl Unpin + Stream<Item = Result<String, io::Error>> {
    let strm = tokio_stream::wrappers::ReadDirStream::new(dirents);
    strm.map(|rdirent| {
        rdirent.and_then(|dirent| {
            dirent
                .file_name()
                .into_string()
                .map_err(|_| io::Error::other("invalid filename"))
        })
    })
}

pub async fn is_dir(dirent: &tokio::fs::DirEntry) -> Result<bool, io::Error> {
    let ft = dirent.file_type().await?;
    Ok(ft.is_dir())
}

pub async fn is_file(dirent: &tokio::fs::DirEntry) -> Result<bool, io::Error> {
    let ft = dirent.file_type().await?;
    Ok(ft.is_file())
}

#[async_trait::async_trait]
impl KvStore for SimpleFsKvStoreString {
    /// The name of the dir.
    type Bucket = String;

    /// The name of the file in the dir(bucket).
    type Key = String;

    /// The content of the file.
    type Val = String;

    type Error = io::Error;

    /// The names of the dirs.
    type Buckets = Pin<Box<dyn Send + Unpin + Stream<Item = Result<Self::Key, Self::Error>>>>;

    /// The names of the files in the dir.
    type Keys = Box<dyn Send + Unpin + Stream<Item = Result<Self::Key, Self::Error>>>;

    type BucketArray = StringArray;
    type KeyArray = StringArray;
    type ValArray = StringArray;

    async fn get_val(&self, bkt: &Self::Bucket, key: &Self::Key) -> Result<Self::Val, Self::Error> {
        let dpat: &Path = Path::new(bkt);
        let full = dpat.join(key);
        let f = tokio::fs::File::open(&full)
            .await
            .map_err(|e| format!("unable to open the file {full:#?}: {e}"))
            .map_err(io::Error::other)?;

        let mut limited = f.take(self.max_file_size);
        let mut contents = String::new();
        limited.read_to_string(&mut contents).await?;
        Ok(contents)
    }

    async fn get_buckets(&self) -> Result<Self::Buckets, Self::Error> {
        let rpat: &Path = Path::new(&self.root_dir);
        let dirents = tokio::fs::read_dir(rpat).await?;
        let strm = tokio_stream::wrappers::ReadDirStream::new(dirents);
        let dirs = strm.filter_map(|rdirent| async move {
            match rdirent {
                Err(e) => Some(Err(e)),
                Ok(dirent) => match is_dir(&dirent).await {
                    Ok(true) => Some(Ok(dirent)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                },
            }
        });
        let dirs = Box::pin(dirs);
        let names = dirs.map(|rdirent| {
            rdirent.and_then(|dirent| {
                dirent
                    .path()
                    .to_str()
                    .map(|s| s.to_string())
                    .ok_or(io::Error::other("invalid bucket name"))
            })
        });
        Ok(Box::pin(names))
    }

    async fn get_keys(&self, bkt: &Self::Bucket) -> Result<Self::Keys, Self::Error> {
        let bucket_path = Path::new(bkt);
        let dirents = tokio::fs::read_dir(&bucket_path)
            .await
            .map_err(|e| format!("unable to read the dir {bucket_path:#?}: {e}"))
            .map_err(io::Error::other)?;
        let strm = tokio_stream::wrappers::ReadDirStream::new(dirents);
        let files = strm.filter_map(|rdirent| async move {
            match rdirent {
                Err(e) => Some(Err(e)),
                Ok(dirent) => match is_file(&dirent).await {
                    Ok(true) => Some(Ok(dirent)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                },
            }
        });
        let files = Box::pin(files);
        let names = files.map(|rdirent| {
            rdirent.and_then(|dirent| {
                dirent
                    .file_name()
                    .into_string()
                    .map_err(|_| io::Error::other("invalid filename"))
            })
        });
        Ok(Box::new(names))
    }

    fn schema(&self) -> Result<SchemaRef, Self::Error> {
        Ok(Schema::new(vec![
            Field::new("bucket", DataType::Utf8, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ])
        .into())
    }

    fn bucket2arr(b: &Self::Bucket, sz: usize) -> Self::BucketArray {
        let vec = vec![b.clone(); sz];
        StringArray::from(vec)
    }

    async fn buckets2array(buckets: Self::Buckets) -> Result<Self::BucketArray, Self::Error> {
        buckets
            .try_fold(Vec::new(), |mut acc, bucket| async move {
                acc.push(bucket);
                Ok(acc)
            })
            .await
            .map(|v: Vec<String>| StringArray::from(v))
    }

    async fn keys2array(keys: Self::Keys) -> Result<Self::KeyArray, Self::Error> {
        keys.try_fold(vec![], |mut state, next| async move {
            state.push(next);
            Ok(state)
        })
        .await
        .map(|v: Vec<String>| StringArray::from(v))
    }

    async fn keys2pairs(
        &self,
        bkt: &Self::Bucket,
        keys: Self::Keys,
    ) -> Result<(Self::KeyArray, Self::ValArray), Self::Error> {
        let karr: Self::KeyArray = Self::keys2array(keys).await?;
        let mut buf: String = String::new();
        let klen: usize = Array::len(&karr);
        let mut vals: Vec<_> = Vec::with_capacity(klen);
        for okey in &karr {
            let key: &str = okey.unwrap_or_default();
            buf.clear();
            buf.push_str(key);
            let val: Self::Val = self.get_val(bkt, &buf).await?;
            vals.push(val);
        }
        let varr = StringArray::from(vals);
        Ok((karr, varr))
    }
}

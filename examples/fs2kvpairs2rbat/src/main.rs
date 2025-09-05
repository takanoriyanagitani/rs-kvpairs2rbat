use std::env;
use std::io;
use std::process::ExitCode;

use rs_kvpairs2rbat::arrow;
use rs_kvpairs2rbat::futures;

use futures::StreamExt;

use arrow::record_batch::RecordBatch;

use rs_kvpairs2rbat::core::KvStore;
use rs_kvpairs2rbat::kvstore::fs::kvs::SimpleFsKvStoreString;

fn env2root_dir() -> Result<String, io::Error> {
    env::var("BUCKETS_ROOT_DIR").map_err(io::Error::other)
}

fn env2max_file_size() -> Result<u64, io::Error> {
    let value = env::var("MAX_FILE_SIZE").map_err(io::Error::other)?;
    str::parse(&value).map_err(io::Error::other)
}

fn print_batch(b: &RecordBatch) -> Result<(), io::Error> {
    println!("{b:#?}");
    Ok(())
}

async fn sub() -> Result<(), io::Error> {
    let root_dir: String = env2root_dir()?;
    let max_file_size: u64 = env2max_file_size()?;

    let sfks2 = SimpleFsKvStoreString {
        root_dir,
        max_file_size,
    };

    let mut buckets = sfks2.get_buckets().await?;
    let orbkt: Option<Result<String, _>> = buckets.next().await;
    let rbkt: Result<String, _> = orbkt.ok_or(io::Error::other("no bucket found"))?;
    let bkt: String = rbkt?;

    let rbat: RecordBatch = sfks2.bucket2batch(&bkt).await.map_err(io::Error::other)?;
    print_batch(&rbat)?;

    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match sub().await {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

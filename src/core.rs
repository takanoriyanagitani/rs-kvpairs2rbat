use std::sync::Arc;

use futures::Stream;

use arrow::array::Array;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

#[async_trait::async_trait]
pub trait KvStore: Send + Sync + 'static {
    type Bucket: Eq + Clone + Send + Sync + 'static;
    type Key: Eq + Send + Sync + 'static;
    type Val: Send + Sync + 'static;

    type BucketArray: Array + 'static;
    type KeyArray: Array + 'static;
    type ValArray: Array + 'static;

    type Error: std::error::Error + Send + Sync + 'static;

    type Buckets: Stream<Item = Result<Self::Bucket, Self::Error>> + Send + 'static;
    type Keys: Stream<Item = Result<Self::Key, Self::Error>> + Send + 'static;

    async fn get_val(&self, bkt: &Self::Bucket, key: &Self::Key) -> Result<Self::Val, Self::Error>;

    async fn get_buckets(&self) -> Result<Self::Buckets, Self::Error>;
    async fn get_keys(&self, bkt: &Self::Bucket) -> Result<Self::Keys, Self::Error>;

    fn schema(&self) -> Result<SchemaRef, Self::Error>;

    async fn buckets2array(buckets: Self::Buckets) -> Result<Self::BucketArray, Self::Error>;
    async fn keys2array(keys: Self::Keys) -> Result<Self::KeyArray, Self::Error>;

    async fn keys2pairs(
        &self,
        bkt: &Self::Bucket,
        keys: Self::Keys,
    ) -> Result<(Self::KeyArray, Self::ValArray), Self::Error>;

    fn bucket2arr(b: &Self::Bucket, sz: usize) -> Self::BucketArray;

    fn arr2batch(
        b: Self::BucketArray,
        k: Self::KeyArray,
        v: Self::ValArray,
        sch: SchemaRef,
    ) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(sch, vec![Arc::new(b), Arc::new(k), Arc::new(v)])
    }

    fn map_to_arrow_error(e: Self::Error) -> ArrowError {
        ArrowError::ExternalError(Box::new(e))
    }

    async fn bucket2batch(&self, bkt: &Self::Bucket) -> Result<RecordBatch, ArrowError> {
        let keys: Self::Keys = self.get_keys(bkt).await.map_err(Self::map_to_arrow_error)?;
        let (karr, varr) = self
            .keys2pairs(bkt, keys)
            .await
            .map_err(Self::map_to_arrow_error)?;

        let klen: usize = Array::len(&karr);
        let vlen: usize = Array::len(&varr);

        let barr: Self::BucketArray = Self::bucket2arr(bkt, klen);
        let sch = self.schema().map_err(Self::map_to_arrow_error)?;

        let blen = Array::len(&barr);
        Self::arr2batch(barr, karr, varr, sch).map_err(|e| {
            ArrowError::ExternalError(Box::new(std::io::Error::other(format!(
                "unable to create a batch. size(bucket/key/val) = {blen}/{klen}/{vlen}: {e}"
            ))))
        })
    }
}

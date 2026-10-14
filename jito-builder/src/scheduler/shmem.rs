//! Shared-memory primitives for Agave IPC scheduling surface

use {
    agave_scheduler_bindings::{
        ExecutionResponseRegion, ExecutionWorkerToPackMessage, MAX_TRANSACTIONS_PER_MESSAGE,
        SharablePubkeys, SharableTransactionBatchRegion, SharableTransactionRegion,
        processed_codes, worker_message_types::ExecutionResponse,
    },
    rts_alloc::Allocator,
    solana_pubkey::Pubkey,
    std::{mem::size_of, ptr::copy_nonoverlapping, slice},
};

pub const MAX_ALLOCATION_SIZE: usize = 4096;

pub trait Free {
    fn free(&self, alloc: &Allocator);
    fn free_full(&self, alloc: &Allocator) {
        self.free(alloc);
    }
}

pub trait Slice<T> {
    fn slice<'a>(&self, alloc: &'a Allocator) -> &'a [T];
}

pub fn allocate(data: impl AsRef<[u8]>, alloc: &Allocator) -> SharableTransactionRegion {
    let data = data.as_ref();
    assert!(!data.is_empty(), "data must be non-empty");
    assert!(
        data.len() <= MAX_ALLOCATION_SIZE,
        "data exceeds MAX_ALLOCATION_SIZE"
    );
    let length = data.len() as u32;
    let ptr = alloc
        .allocate(length)
        .expect("SHM allocator should have capacity");
    let offset = unsafe {
        copy_nonoverlapping(data.as_ptr(), ptr.as_ptr(), data.len());
        alloc.offset(ptr)
    };
    SharableTransactionRegion { offset, length }
}

pub fn allocate_batch<I>(txs: I, alloc: &Allocator) -> SharableTransactionBatchRegion
where
    I: IntoIterator<Item = SharableTransactionRegion>,
    I::IntoIter: ExactSizeIterator,
{
    let iter = txs.into_iter();
    assert!(iter.len() != 0, "batch must be non-empty");
    assert!(
        iter.len() <= MAX_TRANSACTIONS_PER_MESSAGE,
        "batch exceeds MAX_TRANSACTIONS_PER_MESSAGE"
    );
    let num_transactions = iter.len() as u8;
    let bytes = iter
        .len()
        .saturating_mul(size_of::<SharableTransactionRegion>()) as u32;
    let batch_ptr = alloc
        .allocate(bytes)
        .expect("SHM allocator should have capacity");
    let slots = batch_ptr.as_ptr() as *mut SharableTransactionRegion;
    for (i, item) in iter.enumerate() {
        unsafe { slots.add(i).write(item) };
    }
    let transactions_offset = unsafe { alloc.offset(batch_ptr) };
    SharableTransactionBatchRegion {
        num_transactions,
        transactions_offset,
    }
}

impl Free for SharableTransactionRegion {
    fn free(&self, alloc: &Allocator) {
        unsafe { alloc.free_offset(self.offset) };
    }
}

impl Free for SharableTransactionBatchRegion {
    fn free(&self, alloc: &Allocator) {
        unsafe { alloc.free_offset(self.transactions_offset) };
    }

    fn free_full(&self, alloc: &Allocator) {
        for region in <Self as Slice<SharableTransactionRegion>>::slice(self, alloc) {
            region.free(alloc);
        }
        self.free(alloc);
    }
}

impl Free for ExecutionResponseRegion {
    fn free(&self, alloc: &Allocator) {
        if self.num_transaction_responses != 0 {
            unsafe { alloc.free_offset(self.transaction_responses_offset) };
        }
    }

    fn free_full(&self, alloc: &Allocator) {
        self.free(alloc);
    }
}

impl Free for SharablePubkeys {
    fn free(&self, alloc: &Allocator) {
        if self.num_pubkeys != 0 {
            unsafe { alloc.free_offset(self.offset) };
        }
    }
}

impl Free for ExecutionResponse {
    fn free(&self, _alloc: &Allocator) {}
}

impl Free for ExecutionWorkerToPackMessage {
    fn free(&self, alloc: &Allocator) {
        self.batch.free(alloc);
        if self.processed_code == processed_codes::PROCESSED {
            self.responses.free(alloc);
        }
    }

    fn free_full(&self, alloc: &Allocator) {
        self.batch.free_full(alloc);
        if self.processed_code == processed_codes::PROCESSED {
            self.responses.free_full(alloc);
        }
    }
}

macro_rules! impl_slice {
    ($container:ty, $elem:ty, $offset:ident, $count:ident) => {
        impl Slice<$elem> for $container {
            fn slice<'a>(&self, alloc: &'a Allocator) -> &'a [$elem] {
                if self.$count == 0 {
                    return &[];
                }
                unsafe {
                    let data = alloc.ptr_from_offset(self.$offset);
                    slice::from_raw_parts(data.as_ptr() as *const $elem, self.$count as usize)
                }
            }
        }
    };
}

impl_slice!(
    SharableTransactionBatchRegion,
    SharableTransactionRegion,
    transactions_offset,
    num_transactions
);
impl_slice!(
    ExecutionResponseRegion,
    ExecutionResponse,
    transaction_responses_offset,
    num_transaction_responses
);
impl_slice!(SharablePubkeys, Pubkey, offset, num_pubkeys);
impl_slice!(SharableTransactionRegion, u8, offset, length);

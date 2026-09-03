// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use lightning::util::persist::{KVStore, PageToken, PaginatedKVStore, PaginatedListResponse};

use crate::types::{DynStore, DynStoreTrait};

pub(crate) enum StoreOperation {
	Write { primary_namespace: String, secondary_namespace: String, key: String, value: Vec<u8> },
	Remove { primary_namespace: String, secondary_namespace: String, key: String },
}

pub(crate) trait AtomicBatchStore: PaginatedKVStore + Send + Sync {
	fn write_batch(
		&self, operations: Vec<StoreOperation>,
	) -> Pin<Box<dyn Future<Output = Result<(), bitcoin::io::Error>> + Send + 'static>>;
}

struct BatchState {
	operations: Vec<StoreOperation>,
	phase: CommitPhase,
}

#[derive(Clone, Copy, PartialEq)]
enum CommitPhase {
	Idle,
	Operation,
	Commit,
}

struct CommitState {
	batch: Mutex<BatchState>,
	idle: Condvar,
	generation: AtomicU64,
	failed: AtomicBool,
	notify: tokio::sync::Notify,
}

pub(crate) struct AtomicDynStoreWrapper<T: AtomicBatchStore> {
	store: Arc<T>,
	commit_state: Arc<CommitState>,
}

impl<T: AtomicBatchStore> AtomicDynStoreWrapper<T> {
	pub(crate) fn new(store: T) -> Self {
		Self {
			store: Arc::new(store),
			commit_state: Arc::new(CommitState {
				batch: Mutex::new(BatchState { operations: Vec::new(), phase: CommitPhase::Idle }),
				idle: Condvar::new(),
				generation: AtomicU64::new(0),
				failed: AtomicBool::new(false),
				notify: tokio::sync::Notify::new(),
			}),
		}
	}
}

impl<T: AtomicBatchStore + 'static> DynStoreTrait for AtomicDynStoreWrapper<T> {
	fn read_async(
		&self, primary_namespace: &str, secondary_namespace: &str, key: &str,
	) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, bitcoin::io::Error>> + Send + 'static>> {
		Box::pin(KVStore::read(&*self.store, primary_namespace, secondary_namespace, key))
	}

	fn write_async(
		&self, primary_namespace: &str, secondary_namespace: &str, key: &str, buf: Vec<u8>,
	) -> Pin<Box<dyn Future<Output = Result<(), bitcoin::io::Error>> + Send + 'static>> {
		self.commit_state.batch.lock().expect("lock").operations.push(StoreOperation::Write {
			primary_namespace: primary_namespace.to_string(),
			secondary_namespace: secondary_namespace.to_string(),
			key: key.to_string(),
			value: buf,
		});
		Box::pin(async { Ok(()) })
	}

	fn remove_async(
		&self, primary_namespace: &str, secondary_namespace: &str, key: &str, _lazy: bool,
	) -> Pin<Box<dyn Future<Output = Result<(), bitcoin::io::Error>> + Send + 'static>> {
		self.commit_state.batch.lock().expect("lock").operations.push(StoreOperation::Remove {
			primary_namespace: primary_namespace.to_string(),
			secondary_namespace: secondary_namespace.to_string(),
			key: key.to_string(),
		});
		Box::pin(async { Ok(()) })
	}

	fn list_async(
		&self, primary_namespace: &str, secondary_namespace: &str,
	) -> Pin<Box<dyn Future<Output = Result<Vec<String>, bitcoin::io::Error>> + Send + 'static>> {
		Box::pin(KVStore::list(&*self.store, primary_namespace, secondary_namespace))
	}

	fn list_paginated_async(
		&self, primary_namespace: &str, secondary_namespace: &str, page_token: Option<PageToken>,
	) -> Pin<
		Box<
			dyn Future<Output = Result<PaginatedListResponse, bitcoin::io::Error>> + Send + 'static,
		>,
	> {
		Box::pin(PaginatedKVStore::list_paginated(
			&*self.store,
			primary_namespace,
			secondary_namespace,
			page_token,
		))
	}

	fn commit_async(
		&self,
	) -> Pin<Box<dyn Future<Output = Result<(), bitcoin::io::Error>> + Send + 'static>> {
		let operations =
			core::mem::take(&mut self.commit_state.batch.lock().expect("lock").operations);
		let commit = self.store.write_batch(operations);
		let commit_state = Arc::clone(&self.commit_state);
		Box::pin(async move {
			match commit.await {
				Ok(()) => {
					commit_state.generation.fetch_add(1, Ordering::AcqRel);
					commit_state.notify.notify_waiters();
					Ok(())
				},
				Err(e) => {
					commit_state.failed.store(true, Ordering::Release);
					commit_state.notify.notify_waiters();
					Err(e)
				},
			}
		})
	}

	fn commit_generation(&self) -> u64 {
		self.commit_state.generation.load(Ordering::Acquire)
	}

	fn wait_for_commit_async(
		&self, after_generation: u64,
	) -> Pin<Box<dyn Future<Output = Result<(), bitcoin::io::Error>> + Send + 'static>> {
		let commit_state = Arc::clone(&self.commit_state);
		Box::pin(async move {
			loop {
				let notified = commit_state.notify.notified();
				if commit_state.failed.load(Ordering::Acquire) {
					return Err(bitcoin::io::Error::new(
						bitcoin::io::ErrorKind::Other,
						"atomic persistence batch failed",
					));
				}
				if commit_state.generation.load(Ordering::Acquire) > after_generation {
					return Ok(());
				}
				notified.await;
			}
		})
	}

	fn begin_operation(&self) {
		let mut batch = self.commit_state.batch.lock().expect("lock");
		while batch.phase != CommitPhase::Idle {
			batch = self.commit_state.idle.wait(batch).expect("lock");
		}
		batch.phase = CommitPhase::Operation;
	}

	fn end_operation(&self) {
		let mut batch = self.commit_state.batch.lock().expect("lock");
		debug_assert!(batch.phase == CommitPhase::Operation);
		batch.phase = CommitPhase::Idle;
		self.commit_state.idle.notify_all();
	}

	fn begin_atomic_commit(&self) {
		let mut batch = self.commit_state.batch.lock().expect("lock");
		while batch.phase != CommitPhase::Idle {
			batch = self.commit_state.idle.wait(batch).expect("lock");
		}
		batch.phase = CommitPhase::Commit;
	}

	fn end_atomic_commit(&self) {
		let mut batch = self.commit_state.batch.lock().expect("lock");
		debug_assert!(batch.phase == CommitPhase::Commit);
		batch.phase = CommitPhase::Idle;
		self.commit_state.idle.notify_all();
	}
}

pub(crate) struct PersistenceOperationGuard {
	store: Option<Arc<DynStore>>,
	commit_generation: u64,
}

impl PersistenceOperationGuard {
	pub(crate) fn new(store: Arc<DynStore>) -> Self {
		DynStoreTrait::begin_operation(&*store);
		let commit_generation = DynStoreTrait::commit_generation(&*store);
		Self { store: Some(store), commit_generation }
	}

	pub(crate) async fn wait_for_commit(mut self) -> Result<(), bitcoin::io::Error> {
		let store = self.store.take().expect("operation guard must hold a store");
		DynStoreTrait::end_operation(&*store);
		DynStoreTrait::wait_for_commit_async(&*store, self.commit_generation).await
	}
}

impl Drop for PersistenceOperationGuard {
	fn drop(&mut self) {
		if let Some(store) = self.store.take() {
			DynStoreTrait::end_operation(&*store);
		}
	}
}

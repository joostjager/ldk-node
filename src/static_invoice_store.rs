use crate::{hex_utils, rate_limiter::RateLimiter, types::DynStore};
use bitcoin::hashes::{sha256, Hash};
use lightning::{offers::static_invoice::StaticInvoice, util::ser::Writeable};
use std::{
	sync::{Arc, Mutex},
	time::Duration,
};

pub(crate) struct StaticInvoiceStore {
	kv_store: Arc<DynStore>,
	request_rate_limiter: Mutex<RateLimiter>,
	persist_rate_limiter: Mutex<RateLimiter>,
}

impl StaticInvoiceStore {
	// Static invoices are stored at "static_invoices/<sha256(recipient_id)>/<invoice_slot>".
	//
	// Example:
	// static_invoices/039058c6f2c0cb492c533b0a4d14ef77cc0f78abccced5287d84a1a2011cfb81/001f
	const PRIMARY_NAMESPACE: &str = "static_invoices";

	const RATE_LIMITER_BUCKET_CAPACITY: u32 = 5;
	const RATE_LIMITER_REFILL_INTERVAL: Duration = Duration::from_millis(100);
	const RATE_LIMITER_MAX_IDLE: Duration = Duration::from_secs(600);

	pub(crate) fn new(kv_store: Arc<DynStore>) -> Self {
		Self {
			kv_store,
			request_rate_limiter: Mutex::new(RateLimiter::new(
				Self::RATE_LIMITER_BUCKET_CAPACITY,
				Self::RATE_LIMITER_REFILL_INTERVAL,
				Self::RATE_LIMITER_MAX_IDLE,
			)),
			persist_rate_limiter: Mutex::new(RateLimiter::new(
				Self::RATE_LIMITER_BUCKET_CAPACITY,
				Self::RATE_LIMITER_REFILL_INTERVAL,
				Self::RATE_LIMITER_MAX_IDLE,
			)),
		}
	}

	fn check_rate_limit(
		limiter: &Mutex<RateLimiter>, recipient_id: &[u8],
	) -> Result<(), lightning::io::Error> {
		let mut limiter = limiter.lock().unwrap();
		if !limiter.allow(recipient_id) {
			Err(lightning::io::Error::new(lightning::io::ErrorKind::Other, "Rate limit exceeded"))
		} else {
			Ok(())
		}
	}

	pub(crate) async fn handle_static_invoice_requested(
		&self, recipient_id: Vec<u8>, invoice_slot: u16,
	) -> Result<Option<StaticInvoice>, lightning::io::Error> {
		Self::check_rate_limit(&self.request_rate_limiter, &recipient_id)?;

		let (secondary_namespace, key) = Self::get_storage_location(invoice_slot, recipient_id);

		self.kv_store.read(Self::PRIMARY_NAMESPACE, &secondary_namespace, &key).and_then(|data| {
			data.try_into().map(Some).map_err(|e| {
				lightning::io::Error::new(
					lightning::io::ErrorKind::InvalidData,
					format!("Failed to parse static invoice: {:?}", e),
				)
			})
		})
	}

	pub(crate) async fn handle_persist_static_invoice(
		&self, invoice: StaticInvoice, invoice_slot: u16, recipient_id: Vec<u8>,
	) -> Result<(), lightning::io::Error> {
		Self::check_rate_limit(&self.persist_rate_limiter, &recipient_id)?;

		let (secondary_namespace, key) = Self::get_storage_location(invoice_slot, recipient_id);

		let mut buf = Vec::new();
		invoice.write(&mut buf)?;

		self.kv_store.write(Self::PRIMARY_NAMESPACE, &secondary_namespace, &key, buf)
	}

	fn get_storage_location(invoice_slot: u16, recipient_id: Vec<u8>) -> (String, String) {
		let hash = sha256::Hash::hash(&recipient_id).to_byte_array();
		let secondary_namespace = hex_utils::to_string(&hash);

		let key = hex_utils::to_string(&invoice_slot.to_be_bytes());
		(secondary_namespace, key)
	}
}

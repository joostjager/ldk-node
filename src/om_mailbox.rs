use std::{
	collections::{hash_map::Entry, HashMap, VecDeque},
	sync::{Arc, Mutex},
};

use crate::types::OnionMessenger;

pub(crate) struct OnionMessageMailbox {
	map: Mutex<HashMap<bitcoin::secp256k1::PublicKey, VecDeque<lightning::ln::msgs::OnionMessage>>>,
	onion_messenger: Arc<OnionMessenger>,
}

impl OnionMessageMailbox {
	const MAX_MESSAGES_PER_PEER: usize = 100;

	pub fn new(onion_messenger: Arc<OnionMessenger>) -> Self {
		Self { map: Mutex::new(HashMap::new()), onion_messenger }
	}

	pub(crate) fn onion_message_intercepted(
		&self, peer_node_id: bitcoin::secp256k1::PublicKey,
		message: lightning::ln::msgs::OnionMessage,
	) {
		let mut map = self.map.lock().unwrap();

		let queue = map.entry(peer_node_id).or_insert_with(VecDeque::new);
		if queue.len() >= Self::MAX_MESSAGES_PER_PEER {
			queue.pop_front();
		}
		queue.push_back(message);
	}

	pub(crate) fn onion_message_peer_connected(&self, peer_node_id: bitcoin::secp256k1::PublicKey) {
		let mut map = self.map.lock().unwrap();

		let queue = match map.entry(peer_node_id) {
			Entry::Occupied(entry) => entry.into_mut(),
			Entry::Vacant(_) => return,
		};

		while let Some(message) = queue.pop_front() {
			let _ = self.onion_messenger.forward_onion_message(message, &peer_node_id);
		}
	}
}

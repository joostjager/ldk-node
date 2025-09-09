use std::{
	collections::{hash_map::Entry, HashMap, VecDeque},
	sync::Mutex,
};

use lightning::ln::msgs::OnionMessage;

pub(crate) struct OnionMessageMailbox {
	map: Mutex<HashMap<bitcoin::secp256k1::PublicKey, VecDeque<lightning::ln::msgs::OnionMessage>>>,
}

impl OnionMessageMailbox {
	const MAX_MESSAGES_PER_PEER: usize = 100;
	const MAX_PEERS: usize = 100;

	pub fn new() -> Self {
		Self { map: Mutex::new(HashMap::new()) }
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

		// Enforce a peers limit. If exceeded, evict the peer with the longest queue.
		if map.len() > Self::MAX_PEERS {
			let peer_to_remove = map
				.iter()
				.max_by_key(|(_, queue)| queue.len())
				.map(|(peer, _)| peer.clone())
				.unwrap();

			map.remove(&peer_to_remove);
		}
	}

	pub(crate) fn onion_message_peer_connected(
		&self, peer_node_id: bitcoin::secp256k1::PublicKey,
	) -> Vec<OnionMessage> {
		let mut map = self.map.lock().unwrap();

		match map.entry(peer_node_id) {
			Entry::Occupied(mut entry) => {
				let queue = std::mem::take(entry.get_mut());
				queue.into()
			},
			Entry::Vacant(_) => Vec::new(),
		}
	}
}

mod tests {
	use bitcoin::{
		key::Secp256k1,
		secp256k1::{PublicKey, SecretKey},
	};
	use lightning::onion_message;

	use crate::om_mailbox::OnionMessageMailbox;

	#[test]
	fn onion_message_mailbox() {
		let mailbox = OnionMessageMailbox::new();

		let secp = Secp256k1::new();
		let sk_bytes = [12; 32];
		let sk = SecretKey::from_slice(&sk_bytes).unwrap();
		let peer_node_id = PublicKey::from_secret_key(&secp, &sk);

		let blinding_sk = SecretKey::from_slice(&[13; 32]).unwrap();
		let blinding_point = PublicKey::from_secret_key(&secp, &blinding_sk);

		let message_sk = SecretKey::from_slice(&[13; 32]).unwrap();
		let message_point = PublicKey::from_secret_key(&secp, &message_sk);

		let message = lightning::ln::msgs::OnionMessage {
			blinding_point,
			onion_routing_packet: onion_message::packet::Packet {
				version: 0,
				public_key: message_point,
				hop_data: vec![1, 2, 3],
				hmac: [0; 32],
			},
		};
		mailbox.onion_message_intercepted(peer_node_id, message.clone());

		let messages = mailbox.onion_message_peer_connected(peer_node_id);
		assert_eq!(messages.len(), 1);
		assert_eq!(messages[0], message);

		let messages = mailbox.onion_message_peer_connected(peer_node_id);
		assert_eq!(messages.len(), 0);
	}
}

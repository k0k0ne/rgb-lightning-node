use amplify::map;
use bitcoin::blockdata::locktime::absolute::LockTime;
use bitcoin::network::constants::Network;
use bitcoin::psbt::Psbt;
use bitcoin::secp256k1::Secp256k1;
use bitcoin::{BlockHash, TxOut};
use bitcoin_bech32::WitnessProgram;
use lightning::chain::{chainmonitor, ChannelMonitorUpdateStatus};
use lightning::chain::{BestBlock, Filter, Watch};
use lightning::events::bump_transaction::{BumpTransactionEventHandler, Wallet};
use lightning::events::{Event, PaymentFailureReason, PaymentPurpose};
use lightning::ln::channelmanager::{self, PaymentId, RecentPaymentDetails};
use lightning::ln::channelmanager::{
    ChainParameters, ChannelManagerReadArgs, SimpleArcChannelManager,
};
use lightning::ln::peer_handler::{IgnoringMessageHandler, MessageHandler, SimpleArcPeerManager};
use lightning::ln::{ChannelId, PaymentHash, PaymentPreimage, PaymentSecret};
use lightning::onion_message::messenger::{DefaultMessageRouter, SimpleArcOnionMessenger};
use lightning::rgb_utils::{
    get_rgb_channel_info_pending, is_channel_rgb, parse_rgb_payment_info, read_rgb_transfer_info,
    update_rgb_channel_amount, STATIC_BLINDING, WALLET_ACCOUNT_XPUB_FNAME,
    WALLET_FINGERPRINT_FNAME,
};
use lightning::routing::gossip;
use lightning::routing::gossip::{NodeId, P2PGossipSync};
use lightning::routing::router::DefaultRouter;
use lightning::routing::scoring::{ProbabilisticScorer, ProbabilisticScoringFeeParameters};
use lightning::sign::{
    EntropySource, InMemorySigner, KeysManager, OutputSpender, SpendableOutputDescriptor,
};
use lightning::util::config::UserConfig;
use lightning::util::persist::{
    KVStore, MonitorUpdatingPersister, OUTPUT_SWEEPER_PERSISTENCE_KEY,
    OUTPUT_SWEEPER_PERSISTENCE_PRIMARY_NAMESPACE, OUTPUT_SWEEPER_PERSISTENCE_SECONDARY_NAMESPACE,
};
use lightning::util::ser::{ReadableArgs, Writeable};
use lightning::util::sweep as ldk_sweep;
use lightning::{chain, impl_writeable_tlv_based};
use lightning_background_processor::{process_events_async, GossipSync};
use lightning_block_sync::init;
use lightning_block_sync::poll;
use lightning_block_sync::SpvClient;
use lightning_block_sync::UnboundedCache;
use lightning_net_tokio::SocketDescriptor;
use lightning_persister::fs_store::FilesystemStore;
use rand::{thread_rng, Rng, RngCore};
use rgb_lib::{
    bdk::keys::{bip39::Mnemonic, DerivableKey, ExtendedKey},
    bitcoin::{
        bip32::{ChildNumber, ExtendedPrivKey},
        psbt::PartiallySignedTransaction as RgbLibPsbt,
        secp256k1::Secp256k1 as Secp256k1_30,
        ScriptBuf,
    },
    utils::{get_account_xpub, recipient_id_from_script_buf, script_buf_from_recipient_id},
    wallet::{
        rust_only::{AssetColoringInfo, ColoringInfo},
        AssetIface, DatabaseType, Outpoint, Recipient, TransportEndpoint, Wallet as RgbLibWallet,
        WalletData, WitnessData,
    },
    AssetSchema, ConsignmentExt, ContractId, FileContent, RgbTransfer,
};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::ToSocketAddrs;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, SystemTime};
use time::OffsetDateTime;
use tokio::runtime::Handle;
use tokio::sync::watch::Sender;
use tokio::task::JoinHandle;

use crate::bitcoind::BitcoindClient;
use crate::disk::{
    self, FilesystemLogger, CHANNEL_IDS_FNAME, CHANNEL_PEER_DATA, INBOUND_PAYMENTS_FNAME,
    MAKER_SWAPS_FNAME, OUTBOUND_PAYMENTS_FNAME, OUTPUT_SPENDER_TXES, TAKER_SWAPS_FNAME,
};
use crate::error::APIError;
use crate::rgb::{get_rgb_channel_info_optional, RgbLibWalletWrapper};
use crate::routes::{HTLCStatus, SwapStatus, DUST_LIMIT_MSAT};
use crate::swap::SwapData;
use crate::utils::{
    connect_peer_if_necessary, do_connect_peer, get_current_timestamp, hex_str, AppState,
    StaticState, UnlockedAppState,
};

pub(crate) const FEE_RATE: f32 = 7.0;
pub(crate) const UTXO_SIZE_SAT: u32 = 32000;
pub(crate) const MIN_CHANNEL_CONFIRMATIONS: u8 = 6;

pub(crate) struct LdkBackgroundServices {
    stop_processing: Arc<AtomicBool>,
    peer_manager: Arc<PeerManager>,
    bp_exit: Sender<()>,
    background_processor: Option<JoinHandle<Result<(), std::io::Error>>>,
}

#[derive(Clone, Debug)]
pub(crate) struct PaymentInfo {
    pub(crate) preimage: Option<PaymentPreimage>,
    pub(crate) secret: Option<PaymentSecret>,
    pub(crate) status: HTLCStatus,
    pub(crate) amt_msat: Option<u64>,
}

impl_writeable_tlv_based!(PaymentInfo, {
    (0, preimage, required),
    (2, secret, required),
    (4, status, required),
    (6, amt_msat, required),
});

pub(crate) struct InboundPaymentInfoStorage {
    pub(crate) payments: HashMap<PaymentHash, PaymentInfo>,
}

impl_writeable_tlv_based!(InboundPaymentInfoStorage, {
    (0, payments, required),
});

pub(crate) struct OutboundPaymentInfoStorage {
    pub(crate) payments: HashMap<PaymentId, PaymentInfo>,
}

impl_writeable_tlv_based!(OutboundPaymentInfoStorage, {
    (0, payments, required),
});

pub(crate) struct SwapMap {
    pub(crate) swaps: HashMap<PaymentHash, SwapData>,
}

impl_writeable_tlv_based!(SwapMap, {
    (0, swaps, required),
});

pub(crate) struct ChannelIdsMap {
    pub(crate) channel_ids: HashMap<ChannelId, ChannelId>,
}

impl_writeable_tlv_based!(ChannelIdsMap, {
    (0, channel_ids, required),
});

impl UnlockedAppState {
    pub(crate) fn add_maker_swap(&self, payment_hash: PaymentHash, swap: SwapData) {
        let mut maker_swaps = self.get_maker_swaps();
        maker_swaps.swaps.insert(payment_hash, swap);
        self.save_maker_swaps(maker_swaps);
    }

    pub(crate) fn update_maker_swap_status(&self, payment_hash: &PaymentHash, status: SwapStatus) {
        let mut maker_swaps = self.get_maker_swaps();
        let maker_swap = maker_swaps.swaps.get_mut(payment_hash).unwrap();
        match &status {
            SwapStatus::Succeeded | SwapStatus::Failed | SwapStatus::Expired => {
                maker_swap.completed_at = Some(get_current_timestamp())
            }
            SwapStatus::Pending => maker_swap.initiated_at = Some(get_current_timestamp()),
            SwapStatus::Waiting => panic!("this doesn't make sense: swap starts in Waiting status"),
        }
        maker_swap.status = status;
        self.save_maker_swaps(maker_swaps);
    }

    pub(crate) fn is_maker_swap(&self, payment_hash: &PaymentHash) -> bool {
        self.maker_swaps().contains_key(payment_hash)
    }

    pub(crate) fn add_taker_swap(&self, payment_hash: PaymentHash, swap: SwapData) {
        let mut taker_swaps = self.get_taker_swaps();
        taker_swaps.swaps.insert(payment_hash, swap);
        self.save_taker_swaps(taker_swaps);
    }

    pub(crate) fn update_taker_swap_status(&self, payment_hash: &PaymentHash, status: SwapStatus) {
        let mut taker_swaps = self.get_taker_swaps();
        let taker_swap = taker_swaps.swaps.get_mut(payment_hash).unwrap();
        match &status {
            SwapStatus::Succeeded | SwapStatus::Failed | SwapStatus::Expired => {
                taker_swap.completed_at = Some(get_current_timestamp())
            }
            SwapStatus::Pending => taker_swap.initiated_at = Some(get_current_timestamp()),
            SwapStatus::Waiting => panic!("this doesn't make sense: swap starts in Waiting status"),
        }
        taker_swap.status = status;
        self.save_taker_swaps(taker_swaps);
    }

    pub(crate) fn is_taker_swap(&self, payment_hash: &PaymentHash) -> bool {
        self.taker_swaps().contains_key(payment_hash)
    }

    fn save_maker_swaps(&self, swaps: MutexGuard<SwapMap>) {
        self.fs_store
            .write("", "", MAKER_SWAPS_FNAME, &swaps.encode())
            .unwrap();
    }

    fn save_taker_swaps(&self, swaps: MutexGuard<SwapMap>) {
        self.fs_store
            .write("", "", TAKER_SWAPS_FNAME, &swaps.encode())
            .unwrap();
    }

    pub(crate) fn maker_swaps(&self) -> HashMap<PaymentHash, SwapData> {
        self.get_maker_swaps().swaps.clone()
    }

    pub(crate) fn taker_swaps(&self) -> HashMap<PaymentHash, SwapData> {
        self.get_taker_swaps().swaps.clone()
    }

    pub(crate) fn add_inbound_payment(&self, payment_hash: PaymentHash, payment_info: PaymentInfo) {
        let mut inbound = self.get_inbound_payments();
        inbound.payments.insert(payment_hash, payment_info);
        self.save_inbound_payments(inbound);
    }

    pub(crate) fn add_outbound_payment(&self, payment_id: PaymentId, payment_info: PaymentInfo) {
        let mut outbound = self.get_outbound_payments();
        outbound.payments.insert(payment_id, payment_info);
        self.save_outbound_payments(outbound);
    }

    fn fail_outbound_pending_payments(&self, recent_payments_payment_ids: Vec<PaymentId>) {
        let mut outbound = self.get_outbound_payments();
        for (payment_id, payment_info) in outbound
            .payments
            .iter_mut()
            .filter(|(_, i)| matches!(i.status, HTLCStatus::Pending))
        {
            if !recent_payments_payment_ids.contains(payment_id) {
                payment_info.status = HTLCStatus::Failed;
            }
        }
        self.save_outbound_payments(outbound);
    }

    pub(crate) fn inbound_payments(&self) -> HashMap<PaymentHash, PaymentInfo> {
        self.get_inbound_payments().payments.clone()
    }

    pub(crate) fn outbound_payments(&self) -> HashMap<PaymentId, PaymentInfo> {
        self.get_outbound_payments().payments.clone()
    }

    fn save_inbound_payments(&self, inbound: MutexGuard<InboundPaymentInfoStorage>) {
        self.fs_store
            .write("", "", INBOUND_PAYMENTS_FNAME, &inbound.encode())
            .unwrap();
    }

    fn save_outbound_payments(&self, outbound: MutexGuard<OutboundPaymentInfoStorage>) {
        self.fs_store
            .write("", "", OUTBOUND_PAYMENTS_FNAME, &outbound.encode())
            .unwrap();
    }

    fn upsert_inbound_payment(
        &self,
        payment_hash: PaymentHash,
        status: HTLCStatus,
        preimage: Option<PaymentPreimage>,
        secret: Option<PaymentSecret>,
        amt_msat: Option<u64>,
    ) {
        let mut inbound = self.get_inbound_payments();
        match inbound.payments.entry(payment_hash) {
            Entry::Occupied(mut e) => {
                let payment = e.get_mut();
                payment.status = status;
                payment.preimage = preimage;
                payment.secret = secret;
            }
            Entry::Vacant(e) => {
                e.insert(PaymentInfo {
                    preimage,
                    secret,
                    status,
                    amt_msat,
                });
            }
        }
        self.save_inbound_payments(inbound);
    }

    pub(crate) fn update_outbound_payment(
        &self,
        payment_id: PaymentId,
        status: HTLCStatus,
        preimage: Option<PaymentPreimage>,
    ) -> PaymentInfo {
        let mut outbound = self.get_outbound_payments();
        let outbound_payment = outbound.payments.get_mut(&payment_id).unwrap();
        outbound_payment.status = status;
        outbound_payment.preimage = preimage;
        let payment = (*outbound_payment).clone();
        self.save_outbound_payments(outbound);
        payment
    }

    pub(crate) fn update_outbound_payment_status(&self, payment_id: PaymentId, status: HTLCStatus) {
        let mut outbound = self.get_outbound_payments();
        let payment = outbound.payments.get_mut(&payment_id).unwrap();
        payment.status = status;
        self.save_outbound_payments(outbound);
    }

    pub(crate) fn update_inbound_payment_status(
        &self,
        payment_hash: PaymentHash,
        status: HTLCStatus,
    ) {
        let mut inbound = self.get_inbound_payments();
        let payment = inbound.payments.get_mut(&payment_hash).unwrap();
        payment.status = status;
        self.save_inbound_payments(inbound);
    }

    pub(crate) fn channel_ids(&self) -> HashMap<ChannelId, ChannelId> {
        self.get_channel_ids_map().channel_ids.clone()
    }

    pub(crate) fn add_channel_id(
        &self,
        former_temporary_channel_id: ChannelId,
        channel_id: ChannelId,
    ) {
        let mut channel_ids_map = self.get_channel_ids_map();
        channel_ids_map
            .channel_ids
            .insert(former_temporary_channel_id, channel_id);
        self.save_channel_ids_map(channel_ids_map);
    }

    pub(crate) fn delete_channel_id(&self, channel_id: ChannelId) {
        let mut channel_ids_map = self.get_channel_ids_map();
        if let Some(temporary_channel_id) = channel_ids_map
            .channel_ids
            .clone()
            .into_iter()
            .find_map(|(tmp_chan_id, chan_id)| {
                if chan_id == channel_id {
                    Some(tmp_chan_id)
                } else {
                    None
                }
            })
        {
            channel_ids_map.channel_ids.remove(&temporary_channel_id);
            self.save_channel_ids_map(channel_ids_map);
        }
    }

    fn save_channel_ids_map(&self, channel_ids: MutexGuard<ChannelIdsMap>) {
        self.fs_store
            .write("", "", CHANNEL_IDS_FNAME, &channel_ids.encode())
            .unwrap();
    }
}

type ChainMonitor = chainmonitor::ChainMonitor<
    InMemorySigner,
    Arc<dyn Filter + Send + Sync>,
    Arc<BitcoindClient>,
    Arc<BitcoindClient>,
    Arc<FilesystemLogger>,
    Arc<
        MonitorUpdatingPersister<
            Arc<FilesystemStore>,
            Arc<FilesystemLogger>,
            Arc<KeysManager>,
            Arc<KeysManager>,
        >,
    >,
>;

pub(crate) type GossipVerifier = lightning_block_sync::gossip::GossipVerifier<
    lightning_block_sync::gossip::TokioSpawner,
    Arc<lightning_block_sync::rpc::RpcClient>,
    Arc<FilesystemLogger>,
>;

pub(crate) type PeerManager = SimpleArcPeerManager<
    SocketDescriptor,
    ChainMonitor,
    BitcoindClient,
    BitcoindClient,
    GossipVerifier,
    FilesystemLogger,
>;

pub(crate) type Scorer = ProbabilisticScorer<Arc<NetworkGraph>, Arc<FilesystemLogger>>;

pub(crate) type Router = DefaultRouter<
    Arc<NetworkGraph>,
    Arc<FilesystemLogger>,
    Arc<KeysManager>,
    Arc<RwLock<Scorer>>,
    ProbabilisticScoringFeeParameters,
    Scorer,
>;

pub(crate) type ChannelManager =
    SimpleArcChannelManager<ChainMonitor, BitcoindClient, BitcoindClient, FilesystemLogger>;

pub(crate) type NetworkGraph = gossip::NetworkGraph<Arc<FilesystemLogger>>;

pub(crate) type OnionMessenger =
    SimpleArcOnionMessenger<ChainMonitor, BitcoindClient, BitcoindClient, FilesystemLogger>;

pub(crate) type BumpTxEventHandler = BumpTransactionEventHandler<
    Arc<BitcoindClient>,
    Arc<Wallet<Arc<RgbLibWalletWrapper>, Arc<FilesystemLogger>>>,
    Arc<KeysManager>,
    Arc<FilesystemLogger>,
>;

pub(crate) type OutputSpenderTxes = HashMap<u64, bitcoin::Transaction>;

pub(crate) struct RgbOutputSpender {
    static_state: Arc<StaticState>,
    rgb_wallet_wrapper: Arc<RgbLibWalletWrapper>,
    keys_manager: Arc<KeysManager>,
    fs_store: Arc<FilesystemStore>,
    txes: Arc<Mutex<OutputSpenderTxes>>,
}

pub(crate) type OutputSweeper = ldk_sweep::OutputSweeper<
    Arc<BitcoindClient>,
    Arc<RgbLibWalletWrapper>,
    Arc<BitcoindClient>,
    Arc<dyn Filter + Send + Sync>,
    Arc<FilesystemStore>,
    Arc<FilesystemLogger>,
    Arc<RgbOutputSpender>,
>;

fn _update_rgb_channel_amount(color_source: &Path, payment_hash: &PaymentHash, receiver: bool) {
    let payment_hash_str = hex_str(&payment_hash.0);
    for entry in fs::read_dir(color_source).unwrap() {
        let file = entry.unwrap();
        let file_name = file.file_name();
        let file_name_str = file_name.to_string_lossy();
        let mut file_path_no_ext = file.path().clone();
        file_path_no_ext.set_extension("");
        let file_name_str_no_ext = file_path_no_ext.file_name().unwrap().to_string_lossy();
        if file_name_str.contains(&payment_hash_str) && file_name_str_no_ext != payment_hash_str {
            let rgb_payment_info = parse_rgb_payment_info(&file.path());
            let channel_id_str = file_name_str_no_ext.replace(&payment_hash_str, "");

            if rgb_payment_info.swap_payment && receiver != rgb_payment_info.inbound {
                continue;
            }

            let (offered, received) = if receiver {
                (0, rgb_payment_info.amount)
            } else {
                (rgb_payment_info.amount, 0)
            };
            update_rgb_channel_amount(&channel_id_str, offered, received, color_source, false);
            break;
        }
    }
}

async fn handle_ldk_events(
    event: Event,
    unlocked_state: Arc<UnlockedAppState>,
    static_state: Arc<StaticState>,
) {
    match event {
        Event::FundingGenerationReady {
            temporary_channel_id,
            counterparty_node_id,
            channel_value_satoshis,
            output_script,
            ..
        } => {
            let addr = WitnessProgram::from_scriptpubkey(
                output_script.as_bytes(),
                match static_state.network {
                    Network::Bitcoin => bitcoin_bech32::constants::Network::Bitcoin,
                    Network::Testnet => bitcoin_bech32::constants::Network::Testnet,
                    Network::Regtest => bitcoin_bech32::constants::Network::Regtest,
                    Network::Signet => bitcoin_bech32::constants::Network::Signet,
                    _ => unimplemented!("unsupported network"),
                },
            )
            .expect("Lightning funding tx should always be to a SegWit output");
            let script_buf = ScriptBuf::from_bytes(addr.to_scriptpubkey());

            let is_colored = is_channel_rgb(
                &temporary_channel_id,
                &PathBuf::from(&static_state.color_source),
            );
            let (unsigned_psbt, asset_id, recipient_id) = if is_colored {
                let (rgb_info, _) = get_rgb_channel_info_pending(
                    &temporary_channel_id,
                    &PathBuf::from(&static_state.color_source),
                );

                let channel_rgb_amount: u64 = rgb_info.local_rgb_amount;
                let asset_id = rgb_info.contract_id.to_string();

                let recipient_id =
                    recipient_id_from_script_buf(script_buf, static_state.network.into());

                let recipient_map = map! {
                    asset_id.clone() => vec![Recipient {
                        recipient_id: recipient_id.clone(),
                        witness_data: Some(WitnessData {
                            amount_sat: channel_value_satoshis,
                            blinding: Some(STATIC_BLINDING),
                        }),
                        amount: channel_rgb_amount,
                        transport_endpoints: vec![static_state.proxy_endpoint.clone()]
                }]};

                let unlocked_state_copy = unlocked_state.clone();
                let unsigned_psbt = tokio::task::spawn_blocking(move || {
                    unlocked_state_copy
                        .rgb_send_begin(recipient_map, true, FEE_RATE, MIN_CHANNEL_CONFIRMATIONS)
                        .unwrap()
                })
                .await
                .unwrap();
                (unsigned_psbt, Some(asset_id), Some(recipient_id))
            } else {
                let unsigned_psbt = unlocked_state
                    .rgb_send_btc_begin(addr.to_address(), channel_value_satoshis, FEE_RATE)
                    .unwrap();
                (unsigned_psbt, None, None)
            };

            let signed_psbt = unlocked_state.rgb_sign_psbt(unsigned_psbt).unwrap();
            let psbt = Psbt::from_str(&signed_psbt).unwrap();

            let funding_tx = psbt.clone().extract_tx();
            let funding_txid = funding_tx.txid().to_string();

            let psbt_path = static_state
                .color_source
                .join(format!("psbt_{funding_txid}"));
            fs::write(psbt_path, psbt.to_string()).unwrap();

            if is_colored {
                let asset_id = asset_id.expect("is present");
                let recipient_id = recipient_id.expect("is present");
                let transfers_dir = unlocked_state
                    .rgb_get_transfers_dir()
                    .join(funding_txid.clone());
                let asset_transfer_dir =
                    unlocked_state.rgb_get_asset_transfer_dir(transfers_dir, &asset_id);
                let consignment_path =
                    unlocked_state.rgb_get_send_consignment_path(asset_transfer_dir, &recipient_id);
                let proxy_url = TransportEndpoint::new(static_state.proxy_endpoint.clone())
                    .unwrap()
                    .endpoint;
                let unlocked_state_copy = unlocked_state.clone();
                let res = tokio::task::spawn_blocking(move || {
                    unlocked_state_copy.rgb_post_consignment(
                        &proxy_url,
                        funding_txid.clone(),
                        &consignment_path,
                        funding_txid,
                        Some(0),
                    )
                })
                .await
                .unwrap();

                if let Err(e) = res {
                    tracing::error!("cannot post consignment: {e}");
                    return;
                }
            }

            let channel_manager_copy = unlocked_state.channel_manager.clone();

            // Give the funding transaction back to LDK for opening the channel.
            if channel_manager_copy
                .funding_transaction_generated(
                    &temporary_channel_id,
                    &counterparty_node_id,
                    funding_tx,
                )
                .is_err()
            {
                tracing::error!(
                        "ERROR: Channel went away before we could fund it. The peer disconnected or refused the channel.");
            }
        }
        Event::PaymentClaimable {
            payment_hash,
            purpose,
            amount_msat,
            receiver_node_id: _,
            via_channel_id: _,
            via_user_channel_id: _,
            claim_deadline: _,
            onion_fields: _,
            counterparty_skimmed_fee_msat: _,
        } => {
            tracing::info!(
                "EVENT: received payment from payment hash {} of {} millisatoshis",
                payment_hash,
                amount_msat,
            );
            let payment_preimage = match purpose {
                PaymentPurpose::Bolt11InvoicePayment {
                    payment_preimage, ..
                } => payment_preimage,
                PaymentPurpose::Bolt12OfferPayment {
                    payment_preimage, ..
                } => payment_preimage,
                PaymentPurpose::Bolt12RefundPayment {
                    payment_preimage, ..
                } => payment_preimage,
                PaymentPurpose::SpontaneousPayment(preimage) => Some(preimage),
            };
            unlocked_state
                .channel_manager
                .claim_funds(payment_preimage.unwrap());
        }
        Event::PaymentClaimed {
            payment_hash,
            purpose,
            amount_msat,
            receiver_node_id: _,
            htlcs: _,
            sender_intended_total_msat: _,
        } => {
            tracing::info!(
                "EVENT: claimed payment from payment hash {} of {} millisatoshis",
                payment_hash,
                amount_msat,
            );
            let (payment_preimage, payment_secret) = match purpose {
                PaymentPurpose::Bolt11InvoicePayment {
                    payment_preimage,
                    payment_secret,
                    ..
                } => (payment_preimage, Some(payment_secret)),
                PaymentPurpose::Bolt12OfferPayment {
                    payment_preimage,
                    payment_secret,
                    ..
                } => (payment_preimage, Some(payment_secret)),
                PaymentPurpose::Bolt12RefundPayment {
                    payment_preimage,
                    payment_secret,
                    ..
                } => (payment_preimage, Some(payment_secret)),
                PaymentPurpose::SpontaneousPayment(preimage) => (Some(preimage), None),
            };

            static_state.color_source.lock().unwrap().update_rgb_channel_amount(&payment_hash, true);

            if unlocked_state.is_maker_swap(&payment_hash) {
                unlocked_state.update_maker_swap_status(&payment_hash, SwapStatus::Succeeded);
            } else {
                unlocked_state.upsert_inbound_payment(
                    payment_hash,
                    HTLCStatus::Succeeded,
                    payment_preimage,
                    payment_secret,
                    Some(amount_msat),
                );
            }
        }
        Event::PaymentSent {
            payment_preimage,
            payment_hash,
            fee_paid_msat,
            payment_id,
            ..
        } => {
            static_state.color_source.lock().unwrap().update_rgb_channel_amount(&payment_hash, false);

            if unlocked_state.is_maker_swap(&payment_hash) {
                tracing::info!(
                    "EVENT: successfully swapped payment with hash {} and preimage {}",
                    payment_hash,
                    payment_preimage
                );
                unlocked_state.update_maker_swap_status(&payment_hash, SwapStatus::Succeeded);
            } else {
                let payment = unlocked_state.update_outbound_payment(
                    payment_id.unwrap(),
                    HTLCStatus::Succeeded,
                    Some(payment_preimage),
                );
                tracing::info!(
                    "EVENT: successfully sent payment of {:?} millisatoshis{} from \
                            payment hash {} with preimage {}",
                    payment.amt_msat,
                    if let Some(fee) = fee_paid_msat {
                        format!(" (fee {} msat)", fee)
                    } else {
                        "".to_string()
                    },
                    payment_hash,
                    payment_preimage
                );
            }
        }
        Event::OpenChannelRequest {
            ref temporary_channel_id,
            ref counterparty_node_id,
            ..
        } => {
            let mut random_bytes = [0u8; 16];
            random_bytes
                .copy_from_slice(&unlocked_state.keys_manager.get_secure_random_bytes()[..16]);
            let user_channel_id = u128::from_be_bytes(random_bytes);
            let res = unlocked_state.channel_manager.accept_inbound_channel(
                temporary_channel_id,
                counterparty_node_id,
                user_channel_id,
            );

            if let Err(e) = res {
                tracing::error!(
                    "EVENT: Failed to accept inbound channel ({}) from {}: {:?}",
                    temporary_channel_id,
                    hex_str(&counterparty_node_id.serialize()),
                    e,
                );
            } else {
                tracing::info!(
                    "EVENT: Accepted inbound channel ({}) from {}",
                    temporary_channel_id,
                    hex_str(&counterparty_node_id.serialize()),
                );
            }
        }
        Event::PaymentPathSuccessful { .. } => {}
        Event::PaymentPathFailed { .. } => {}
        Event::ProbeSuccessful { .. } => {}
        Event::ProbeFailed { .. } => {}
        Event::PaymentFailed {
            payment_hash,
            reason,
            payment_id,
            ..
        } => {
            tracing::error!(
                "EVENT: Failed to send payment to payment hash {:?}: {:?}",
                payment_hash,
                if let Some(r) = reason {
                    r
                } else {
                    PaymentFailureReason::RetriesExhausted
                }
            );

            if unlocked_state.is_maker_swap(&payment_hash) {
                unlocked_state.update_maker_swap_status(&payment_hash, SwapStatus::Failed);
            } else {
                unlocked_state.update_outbound_payment_status(payment_id, HTLCStatus::Failed);
            }
        }
        Event::InvoiceRequestFailed { payment_id } => {
            tracing::error!(
                "EVENT: Failed to request invoice to send payment with id {}",
                payment_id,
            );

            unlocked_state.update_outbound_payment_status(payment_id, HTLCStatus::Failed);
        }
        Event::PaymentForwarded {
            prev_channel_id,
            next_channel_id,
            total_fee_earned_msat,
            claim_from_onchain_tx,
            outbound_amount_forwarded_msat,
            skimmed_fee_msat: _,
            prev_user_channel_id: _,
            next_user_channel_id: _,
            outbound_amount_forwarded_rgb,
            inbound_amount_forwarded_rgb,
            payment_hash,
        } => {
            let prev_channel_id_str = prev_channel_id.expect("prev_channel_id").to_string();
            let next_channel_id_str = next_channel_id.expect("next_channel_id").to_string();

            if let Some(outbound_amount_forwarded_rgb) = outbound_amount_forwarded_rgb {
                update_rgb_channel_amount(
                    &next_channel_id_str,
                    outbound_amount_forwarded_rgb,
                    0,
                    &static_state.color_source,
                    false,
                );
            }
            if let Some(inbound_amount_forwarded_rgb) = inbound_amount_forwarded_rgb {
                update_rgb_channel_amount(
                    &prev_channel_id_str,
                    0,
                    inbound_amount_forwarded_rgb,
                    &static_state.color_source,
                    false,
                );
            }

            if unlocked_state.is_taker_swap(&payment_hash) {
                unlocked_state.update_taker_swap_status(&payment_hash, SwapStatus::Succeeded);
            }

            let read_only_network_graph = unlocked_state.network_graph.read_only();
            let nodes = read_only_network_graph.nodes();
            let channels = unlocked_state.channel_manager.list_channels();

            let node_str = |channel_id: &Option<ChannelId>| match channel_id {
                None => String::new(),
                Some(channel_id) => match channels.iter().find(|c| c.channel_id == *channel_id) {
                    None => String::new(),
                    Some(channel) => {
                        match nodes.get(&NodeId::from_pubkey(&channel.counterparty.node_id)) {
                            None => "private node".to_string(),
                            Some(node) => match &node.announcement_info {
                                None => "unnamed node".to_string(),
                                Some(announcement) => {
                                    format!("node {}", announcement.alias)
                                }
                            },
                        }
                    }
                },
            };
            let channel_str = |channel_id: &Option<ChannelId>| {
                channel_id
                    .map(|channel_id| format!(" with channel {}", channel_id))
                    .unwrap_or_default()
            };
            let from_prev_str = format!(
                " from {}{}",
                node_str(&prev_channel_id),
                channel_str(&prev_channel_id)
            );
            let to_next_str = format!(
                " to {}{}",
                node_str(&next_channel_id),
                channel_str(&next_channel_id)
            );

            let from_onchain_str = if claim_from_onchain_tx {
                "from onchain downstream claim"
            } else {
                "from HTLC fulfill message"
            };
            let amt_args = if let Some(v) = outbound_amount_forwarded_msat {
                format!("{}", v)
            } else {
                "?".to_string()
            };
            if let Some(fee_earned) = total_fee_earned_msat {
                tracing::info!(
                    "EVENT: Forwarded payment for {} msat{}{}, earning {} msat {}",
                    amt_args,
                    from_prev_str,
                    to_next_str,
                    fee_earned,
                    from_onchain_str
                );
            } else {
                tracing::info!(
                    "EVENT: Forwarded payment for {} msat{}{}, claiming onchain {}",
                    amt_args,
                    from_prev_str,
                    to_next_str,
                    from_onchain_str
                );
            }
        }
        Event::HTLCHandlingFailed { .. } => {}
        Event::PendingHTLCsForwardable { time_forwardable } => {
            let forwarding_channel_manager = unlocked_state.channel_manager.clone();
            let min = time_forwardable.as_millis() as u64;
            tokio::spawn(async move {
                let millis_to_sleep = thread_rng().gen_range(min..(min * 5));
                tokio::time::sleep(Duration::from_millis(millis_to_sleep)).await;
                forwarding_channel_manager.process_pending_htlc_forwards();
            });
        }
        Event::SpendableOutputs {
            outputs,
            channel_id,
        } => {
            tracing::info!("EVENT: tracking {} spendable outputs", outputs.len(),);

            unlocked_state
                .output_sweeper
                .track_spendable_outputs(outputs, channel_id, false, None)
                .unwrap();
        }
        Event::ChannelPending {
            channel_id,
            counterparty_node_id,
            funding_txo,
            former_temporary_channel_id,
            ..
        } => {
            tracing::info!(
                "EVENT: Channel {} with peer {} is pending awaiting funding lock-in!",
                channel_id,
                hex_str(&counterparty_node_id.serialize()),
            );

            unlocked_state.add_channel_id(former_temporary_channel_id.unwrap(), channel_id);

            let funding_txid = funding_txo.txid.to_string();
            let psbt_path = static_state
                .color_source
                .join(format!("psbt_{funding_txid}"));

            if psbt_path.exists() {
                let psbt_str = fs::read_to_string(psbt_path).unwrap();

                let state_copy = unlocked_state.clone();
                let psbt_str_copy = psbt_str.clone();
                let _txid = tokio::task::spawn_blocking(move || {
                    if is_channel_rgb(&channel_id, &PathBuf::from(&static_state.color_source)) {
                        state_copy.rgb_send_end(psbt_str_copy).unwrap().txid
                    } else {
                        state_copy.rgb_send_btc_end(psbt_str_copy).unwrap()
                    }
                })
                .await
                .unwrap();

                *unlocked_state.rgb_send_lock.lock().unwrap() = false;
            } else {
                // acceptor
                let consignment_path = static_state
                    .color_source
                    .join(format!("consignment_{funding_txid}"));
                if !consignment_path.exists() {
                    return;
                }
                let consignment =
                    RgbTransfer::load_file(consignment_path).expect("successful consignment load");
                let contract_id = consignment.contract_id();
                let schema_id = consignment.schema_id().to_string();
                let asset_schema = AssetSchema::from_schema_id(schema_id).unwrap();

                match unlocked_state.rgb_save_new_asset(&asset_schema, contract_id, None) {
                    Ok(_) => {}
                    Err(e) if e.to_string().contains("UNIQUE constraint failed") => {}
                    Err(e) => panic!("Failed saving asset: {}", e),
                }
            }
        }
        Event::ChannelReady {
            ref channel_id,
            user_channel_id: _,
            ref counterparty_node_id,
            channel_type: _,
        } => {
            tracing::info!(
                "EVENT: Channel {} with peer {} is ready to be used!",
                channel_id,
                hex_str(&counterparty_node_id.serialize()),
            );

            tokio::task::spawn_blocking(move || {
                unlocked_state.rgb_refresh().unwrap();
                unlocked_state.rgb_refresh().unwrap()
            })
            .await
            .unwrap();
        }
        Event::ChannelClosed {
            channel_id,
            reason,
            user_channel_id: _,
            counterparty_node_id,
            channel_capacity_sats: _,
            channel_funding_txo: _,
        } => {
            tracing::info!(
                "EVENT: Channel {} with counterparty {} closed due to: {:?}",
                channel_id,
                counterparty_node_id
                    .map(|id| format!("{}", id))
                    .unwrap_or("".to_owned()),
                reason
            );

            let inbound_payments = unlocked_state.inbound_payments();
            let outbound_payments = unlocked_state.outbound_payments();

            for (payment_hash, payment_info) in &inbound_payments {
                if payment_info.status == HTLCStatus::Pending {
                    unlocked_state.update_inbound_payment_status(*payment_hash, HTLCStatus::Failed);
                }
            }

            for (payment_id, payment_info) in &outbound_payments {
                if payment_info.status == HTLCStatus::Pending {
                    unlocked_state.update_outbound_payment_status(*payment_id, HTLCStatus::Failed);
                }
            }

            unlocked_state.delete_channel_id(channel_id);
        }
        Event::DiscardFunding { channel_id, .. } => {
            // A "real" node should probably "lock" the UTXOs spent in funding transactions until
            // the funding transaction either confirms, or this event is generated.

            *unlocked_state.rgb_send_lock.lock().unwrap() = false;

            unlocked_state.delete_channel_id(channel_id);
        }
        Event::HTLCIntercepted {
            is_swap,
            payment_hash,
            intercept_id,
            inbound_amount_msat,
            expected_outbound_amount_msat,
            inbound_rgb_amount,
            expected_outbound_rgb_amount,
            requested_next_hop_scid,
            prev_short_channel_id,
        } => {
            if !is_swap {
                unlocked_state
                    .channel_manager
                    .fail_intercepted_htlc(intercept_id)
                    .unwrap();
            }

            let get_rgb_info = |channel_id| {
                get_rgb_channel_info_optional(
                    channel_id,
                    &PathBuf::from(&static_state.color_source),
                    true,
                )
                .map(|(rgb_info, _)| {
                    (
                        rgb_info.contract_id,
                        rgb_info.local_rgb_amount,
                        rgb_info.remote_rgb_amount,
                    )
                })
            };

            let inbound_channel = unlocked_state
                .channel_manager
                .list_channels()
                .into_iter()
                .find(|details| details.short_channel_id == Some(prev_short_channel_id))
                .expect("Should always be a valid channel");
            let outbound_channel = unlocked_state
                .channel_manager
                .list_channels()
                .into_iter()
                .find(|details| details.short_channel_id == Some(requested_next_hop_scid))
                .expect("Should always be a valid channel");

            let inbound_rgb_info = get_rgb_info(&inbound_channel.channel_id);
            let outbound_rgb_info = get_rgb_info(&outbound_channel.channel_id);

            tracing::debug!("EVENT: Requested swap with params inbound_msat={} outbound_msat={} inbound_rgb={:?} outbound_rgb={:?} inbound_contract_id={:?}, outbound_contract_id={:?}", inbound_amount_msat, expected_outbound_amount_msat, inbound_rgb_amount, expected_outbound_rgb_amount, inbound_rgb_info.map(|i| i.0), outbound_rgb_info.map(|i| i.0));

            let swaps_lock = unlocked_state.taker_swaps.lock().unwrap();
            let whitelist_swap = match swaps_lock.swaps.get(&payment_hash) {
                None => {
                    tracing::error!("ERROR: rejecting non-whitelisted swap");
                    unlocked_state
                        .channel_manager
                        .fail_intercepted_htlc(intercept_id)
                        .unwrap();
                    return;
                }
                Some(x) => x,
            };

            let mut fail = false;
            if whitelist_swap.swap_info.is_from_btc() {
                let net_msat_diff = expected_outbound_amount_msat.checked_sub(inbound_amount_msat);

                if inbound_rgb_amount != Some(whitelist_swap.swap_info.qty_to)
                    || inbound_rgb_info.map(|x| x.0) != whitelist_swap.swap_info.to_asset
                    || net_msat_diff != Some(whitelist_swap.swap_info.qty_from)
                {
                    fail = true;
                }
            } else if whitelist_swap.swap_info.is_to_btc() {
                let net_msat_diff =
                    inbound_amount_msat.saturating_sub(expected_outbound_amount_msat);

                if expected_outbound_rgb_amount != Some(whitelist_swap.swap_info.qty_from)
                    || outbound_rgb_info.map(|x| x.0) != whitelist_swap.swap_info.from_asset
                    || net_msat_diff != whitelist_swap.swap_info.qty_to
                {
                    fail = true;
                }
            } else {
                let net_msat_diff = inbound_amount_msat.checked_sub(expected_outbound_amount_msat);

                if net_msat_diff != Some(0)
                    || expected_outbound_rgb_amount != Some(whitelist_swap.swap_info.qty_from)
                    || outbound_rgb_info.map(|x| x.0) != whitelist_swap.swap_info.from_asset
                    || inbound_rgb_amount != Some(whitelist_swap.swap_info.qty_to)
                    || inbound_rgb_info.map(|x| x.0) != whitelist_swap.swap_info.to_asset
                {
                    fail = true;
                }
            }

            drop(swaps_lock);

            if fail {
                tracing::error!("ERROR: swap doesn't match the whitelisted info, rejecting it");
                unlocked_state.update_taker_swap_status(&payment_hash, SwapStatus::Failed);
                unlocked_state
                    .channel_manager
                    .fail_intercepted_htlc(intercept_id)
                    .unwrap();
                return;
            }

            tracing::debug!("Swap is whitelisted, forwarding the htlc...");
            unlocked_state.update_taker_swap_status(&payment_hash, SwapStatus::Pending);

            unlocked_state
                .channel_manager
                .forward_intercepted_htlc(
                    intercept_id,
                    channelmanager::NextHopForward::ShortChannelId(requested_next_hop_scid),
                    outbound_channel.counterparty.node_id,
                    expected_outbound_amount_msat,
                    expected_outbound_rgb_amount,
                )
                .expect("Forward should be valid");
        }
        Event::BumpTransaction(event) => unlocked_state.bump_tx_event_handler.handle_event(&event),
        Event::ConnectionNeeded { node_id, addresses } => {
            tokio::spawn(async move {
                for address in addresses {
                    if let Ok(sockaddrs) = address.to_socket_addrs() {
                        for addr in sockaddrs {
                            let pm = Arc::clone(&unlocked_state.peer_manager);
                            if connect_peer_if_necessary(node_id, addr, pm).await.is_ok() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    }
}

impl OutputSpender for RgbOutputSpender {
    fn spend_spendable_outputs<C: bitcoin::secp256k1::Signing>(
        &self,
        descriptors: &[&SpendableOutputDescriptor],
        outputs: Vec<TxOut>,
        change_destination_script: ScriptBuf,
        feerate_sat_per_1000_weight: u32,
        locktime: Option<LockTime>,
        secp_ctx: &Secp256k1<C>,
    ) -> Result<bitcoin::Transaction, ()> {
        let mut hasher = DefaultHasher::new();
        descriptors.hash(&mut hasher);
        let descriptors_hash = hasher.finish();
        let mut txes = self.txes.lock().unwrap();
        if let Some(tx) = txes.get(&descriptors_hash) {
            return Ok(tx.clone());
        }

        let mut vout = 0;
        let mut vanilla_descriptor = true;

        let mut txouts = outputs.clone();
        let mut asset_info: HashMap<ContractId, (u32, u64, String, Vec<Outpoint>)> = map![];

        for outp in descriptors {
            let outpoint = match outp {
                SpendableOutputDescriptor::StaticPaymentOutput(descriptor) => descriptor.outpoint,
                SpendableOutputDescriptor::DelayedPaymentOutput(descriptor) => descriptor.outpoint,
                SpendableOutputDescriptor::StaticOutput { ref outpoint, .. } => *outpoint,
            };

            let txid = outpoint.txid;
            let txid_str = txid.to_string();

            let transfer_info_path = self
                .static_state
                .color_source
                .join(format!("{txid_str}_transfer_info"));
            if !transfer_info_path.exists() {
                continue;
            };
            let transfer_info = read_rgb_transfer_info(&transfer_info_path);
            if transfer_info.rgb_amount == 0 {
                continue;
            }

            vanilla_descriptor = false;

            let closing_height = self
                .rgb_wallet_wrapper
                .get_tx_height(txid_str.clone())
                .map_err(|_| ())?;
            let update_res = self
                .rgb_wallet_wrapper
                .update_witnesses(closing_height.unwrap())
                .unwrap();
            if !update_res.failed.is_empty() {
                return Err(());
            }

            let contract_id = transfer_info.contract_id;

            let mut new_asset = false;
            let recipient_id = if let Some((_, _, recipient_id, _)) = asset_info.get(&contract_id) {
                recipient_id.clone()
            } else {
                new_asset = true;
                let receive_data = self
                    .rgb_wallet_wrapper
                    .witness_receive(vec![self.static_state.proxy_endpoint.clone()])
                    .unwrap();
                let script_pubkey = script_buf_from_recipient_id(receive_data.recipient_id.clone())
                    .unwrap()
                    .unwrap();
                txouts.push(TxOut {
                    value: DUST_LIMIT_MSAT / 1000,
                    script_pubkey,
                });
                receive_data.recipient_id
            };

            let amt_rgb = transfer_info.rgb_amount;

            let input_outpoint = Outpoint {
                txid: txid_str,
                vout: outpoint.index.into(),
            };
            asset_info
                .entry(contract_id)
                .and_modify(|(_, a, _, i)| {
                    *a += amt_rgb;
                    i.push(input_outpoint.clone());
                })
                .or_insert_with(|| (vout, amt_rgb, recipient_id, vec![input_outpoint.clone()]));

            if new_asset {
                vout += 1;
            }
        }

        if vanilla_descriptor {
            return self.keys_manager.spend_spendable_outputs(
                descriptors.as_ref(),
                txouts,
                change_destination_script,
                feerate_sat_per_1000_weight,
                locktime,
                secp_ctx,
            );
        }

        let feerate_sat_per_1000_weight = FEE_RATE as u32 * 250; // 1 sat/vB = 250 sat/kw
        let (psbt, _expected_max_weight) =
            SpendableOutputDescriptor::create_spendable_outputs_psbt(
                secp_ctx,
                descriptors,
                txouts,
                change_destination_script,
                feerate_sat_per_1000_weight,
                locktime,
            )
            .unwrap();

        let mut asset_info_map = map![];
        for (contract_id, (vout, amt_rgb, _, input_outpoints)) in asset_info.clone() {
            asset_info_map.insert(
                contract_id,
                AssetColoringInfo {
                    iface: AssetIface::RGB20,
                    output_map: HashMap::from_iter([(vout, amt_rgb)]),
                    input_outpoints,
                    static_blinding: None,
                },
            );
        }

        let coloring_info = ColoringInfo {
            asset_info_map,
            static_blinding: None,
            nonce: None,
        };

        let mut psbt = RgbLibPsbt::from_str(&psbt.to_string()).unwrap();
        let consignments = self
            .rgb_wallet_wrapper
            .color_psbt_and_consume(&mut psbt, coloring_info)
            .unwrap();

        let mut psbt = Psbt::from_str(&psbt.to_string()).expect("valid transaction");

        psbt = self
            .keys_manager
            .sign_spendable_outputs_psbt(descriptors, psbt, secp_ctx)
            .unwrap();

        let spending_tx = psbt.extract_tx();

        let closing_txid = spending_tx.txid().to_string();

        let handle = Handle::current();
        let _ = handle.enter();

        for consignment in consignments {
            let contract_id = consignment.contract_id();

            let (vout, _, recipient_id, _) = asset_info[&contract_id].clone();

            let consignment_path = self
                .static_state
                .color_source
                .join(format!("consignment_{}", closing_txid.clone()));
            consignment
                .save_file(&consignment_path)
                .expect("successful save");
            let proxy_url = TransportEndpoint::new(self.static_state.proxy_endpoint.clone())
                .unwrap()
                .endpoint;
            let rgb_wallet_wrapper_copy = self.rgb_wallet_wrapper.clone();
            let closing_txid_copy = closing_txid.clone();
            let consignment_path_copy = consignment_path.clone();
            let res = futures::executor::block_on(tokio::task::spawn_blocking(move || {
                rgb_wallet_wrapper_copy.post_consignment(
                    &proxy_url,
                    recipient_id,
                    &consignment_path_copy,
                    closing_txid_copy,
                    Some(vout),
                )
            }));
            if let Err(e) = res {
                tracing::error!("cannot post consignment: {e}");
                return Err(());
            }
            fs::remove_file(&consignment_path).unwrap();
        }

        txes.insert(descriptors_hash, spending_tx.clone());
        self.fs_store
            .write("", "", OUTPUT_SPENDER_TXES, &txes.encode())
            .unwrap();

        Ok(spending_tx)
    }
}

pub(crate) async fn start_ldk(
    app_state: Arc<AppState>,
    mnemonic: Mnemonic,
) -> Result<(LdkBackgroundServices, Arc<UnlockedAppState>), APIError> {
    let static_state = &app_state.static_state;

    let bitcoind_client = static_state.bitcoind_client.clone();
    let color_source = static_state.color_source.clone();
    let color_source_path = PathBuf::from(&color_source);
    let logger = static_state.logger.clone();
    let network = static_state.network;
    let ldk_peer_listening_port = static_state.ldk_peer_listening_port;
    let ldk_announced_listen_addr = static_state.ldk_announced_listen_addr.clone();
    let ldk_announced_node_name = static_state.ldk_announced_node_name;
    let indexer_url = static_state.indexer_url.clone();

    // Initialize the FeeEstimator
    // BitcoindClient implements the FeeEstimator trait, so it'll act as our fee estimator.
    let fee_estimator = bitcoind_client.clone();

    // Initialize the BroadcasterInterface
    // BitcoindClient implements the BroadcasterInterface trait, so it'll act as our transaction
    // broadcaster.
    let broadcaster = bitcoind_client.clone();

    // Initialize the KeysManager
    // The key seed that we use to derive the node privkey (that corresponds to the node pubkey) and
    // other secret key material.
    let xkey: ExtendedKey = mnemonic
        .clone()
        .into_extended_key()
        .expect("a valid key should have been provided");
    let master_xprv = &xkey
        .into_xprv(network)
        .expect("should be possible to get an extended private key");
    let xprv: ExtendedPrivKey = master_xprv
        .ckd_priv(&Secp256k1_30::new(), ChildNumber::Hardened { index: 535 })
        .unwrap();
    let ldk_seed: [u8; 32] = xprv.private_key.secret_bytes();
    let cur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let keys_manager = Arc::new(KeysManager::new(
        &ldk_seed,
        cur.as_secs(),
        cur.subsec_nanos(),
        color_source_path.clone(),
    ));

    // Initialize Persistence
    let fs_store = Arc::new(FilesystemStore::new(color_source.clone()));
    let persister = Arc::new(MonitorUpdatingPersister::new(
        Arc::clone(&fs_store),
        Arc::clone(&logger),
        1000,
        Arc::clone(&keys_manager),
        Arc::clone(&keys_manager),
        color_source_path.clone(),
    ));

    // Initialize the ChainMonitor
    let chain_monitor: Arc<ChainMonitor> = Arc::new(chainmonitor::ChainMonitor::new(
        None,
        Arc::clone(&broadcaster),
        Arc::clone(&logger),
        Arc::clone(&fee_estimator),
        Arc::clone(&persister),
    ));

    // Read ChannelMonitor state from disk
    let mut channelmonitors = persister
        .read_all_channel_monitors_with_updates(&bitcoind_client, &bitcoind_client)
        .unwrap();

    // Poll for the best chain tip, which may be used by the channel manager & spv client
    let polled_chain_tip = init::validate_best_block_header(bitcoind_client.as_ref())
        .await
        .expect("Failed to fetch best block header and best block");

    // Initialize routing ProbabilisticScorer
    let network_graph_path = color_source.join("network_graph");
    let network_graph = Arc::new(disk::read_network(
        &network_graph_path,
        network,
        logger.clone(),
    ));

    let scorer_path = color_source.join("scorer");
    let scorer = Arc::new(RwLock::new(disk::read_scorer(
        &scorer_path,
        Arc::clone(&network_graph),
        Arc::clone(&logger),
    )));

    // Create Router
    let scoring_fee_params = ProbabilisticScoringFeeParameters::default();
    let router = Arc::new(DefaultRouter::new(
        network_graph.clone(),
        logger.clone(),
        keys_manager.clone(),
        scorer.clone(),
        scoring_fee_params,
    ));

    // Initialize the ChannelManager
    let mut user_config = UserConfig::default();
    user_config
        .channel_handshake_limits
        .force_announced_channel_preference = false;
    user_config
        .channel_handshake_config
        .negotiate_anchors_zero_fee_htlc_tx = true;
    user_config.manually_accept_inbound_channels = true;
    let mut restarting_node = true;
    let (channel_manager_blockhash, channel_manager) = {
        if let Ok(mut f) = fs::File::open(color_source.join("manager")) {
            let mut channel_monitor_mut_references = Vec::new();
            for (_, channel_monitor) in channelmonitors.iter_mut() {
                channel_monitor_mut_references.push(channel_monitor);
            }
            let read_args = ChannelManagerReadArgs::new(
                keys_manager.clone(),
                keys_manager.clone(),
                keys_manager.clone(),
                fee_estimator.clone(),
                chain_monitor.clone(),
                broadcaster.clone(),
                router.clone(),
                logger.clone(),
                user_config,
                channel_monitor_mut_references,
                color_source_path.clone(),
            );
            <(BlockHash, ChannelManager)>::read(&mut f, read_args).unwrap()
        } else {
            // We're starting a fresh node.
            restarting_node = false;

            let polled_best_block = polled_chain_tip.to_best_block();
            let polled_best_block_hash = polled_best_block.block_hash;
            let chain_params = ChainParameters {
                network,
                best_block: polled_best_block,
            };
            let fresh_channel_manager = channelmanager::ChannelManager::new(
                fee_estimator.clone(),
                chain_monitor.clone(),
                broadcaster.clone(),
                router.clone(),
                logger.clone(),
                keys_manager.clone(),
                keys_manager.clone(),
                keys_manager.clone(),
                user_config,
                chain_params,
                cur.as_secs() as u32,
                color_source_path.clone(),
            );
            (polled_best_block_hash, fresh_channel_manager)
        }
    };

    // Prepare the RGB wallet
    let mnemonic_str = mnemonic.to_string();
    let bitcoin_network = network.into();
    let account_xpub = get_account_xpub(bitcoin_network, &mnemonic_str).unwrap();
    let data_dir = static_state
        .storage_dir_path
        .clone()
        .to_string_lossy()
        .to_string();
    let mut rgb_wallet = tokio::task::spawn_blocking(move || {
        RgbLibWallet::new(WalletData {
            data_dir,
            bitcoin_network,
            database_type: DatabaseType::Sqlite,
            max_allocations_per_utxo: 1,
            pubkey: account_xpub.to_string(),
            mnemonic: Some(mnemonic.to_string()),
            vanilla_keychain: None,
        })
        .expect("valid rgb-lib wallet")
    })
    .await
    .unwrap();
    let rgb_online = rgb_wallet
        .go_online(false, indexer_url.clone())
        .map_err(|e| APIError::FailedStartingLDK(e.to_string()))?;
    fs::write(
        static_state.storage_dir_path.join(WALLET_FINGERPRINT_FNAME),
        account_xpub.fingerprint().to_string(),
    )
    .expect("able to write");
    fs::write(
        static_state
            .storage_dir_path
            .join(WALLET_ACCOUNT_XPUB_FNAME),
        account_xpub.to_string(),
    )
    .expect("able to write");

    let rgb_wallet_wrapper = Arc::new(RgbLibWalletWrapper::new(
        Arc::new(Mutex::new(rgb_wallet)),
        rgb_online.clone(),
    ));

    // Initialize the OutputSweeper.
    let txes = Arc::new(Mutex::new(disk::read_output_spender_txes(
        &color_source.join(OUTPUT_SPENDER_TXES),
    )));
    let rgb_output_spender = Arc::new(RgbOutputSpender {
        static_state: static_state.clone(),
        rgb_wallet_wrapper: rgb_wallet_wrapper.clone(),
        keys_manager: keys_manager.clone(),
        fs_store: fs_store.clone(),
        txes,
    });
    let (sweeper_best_block, output_sweeper) = match fs_store.read(
        OUTPUT_SWEEPER_PERSISTENCE_PRIMARY_NAMESPACE,
        OUTPUT_SWEEPER_PERSISTENCE_SECONDARY_NAMESPACE,
        OUTPUT_SWEEPER_PERSISTENCE_KEY,
    ) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let sweeper = OutputSweeper::new(
                channel_manager.current_best_block(),
                broadcaster.clone(),
                fee_estimator.clone(),
                None,
                rgb_output_spender,
                rgb_wallet_wrapper.clone(),
                fs_store.clone(),
                logger.clone(),
            );
            (channel_manager.current_best_block(), sweeper)
        }
        Ok(mut bytes) => {
            let read_args = (
                broadcaster.clone(),
                fee_estimator.clone(),
                None,
                rgb_output_spender.clone(),
                rgb_wallet_wrapper.clone(),
                fs_store.clone(),
                logger.clone(),
            );
            let mut reader = std::io::Cursor::new(&mut bytes);
            <(BestBlock, OutputSweeper)>::read(&mut reader, read_args)
                .expect("Failed to deserialize OutputSweeper")
        }
        Err(e) => panic!("Failed to read OutputSweeper with {}", e),
    };

    // Sync ChannelMonitors, ChannelManager and OutputSweeper to chain tip
    let mut chain_listener_channel_monitors = Vec::new();
    let mut cache = UnboundedCache::new();
    let chain_tip = if restarting_node {
        let mut chain_listeners = vec![
            (
                channel_manager_blockhash,
                &channel_manager as &(dyn chain::Listen + Send + Sync),
            ),
            (
                sweeper_best_block.block_hash,
                &output_sweeper as &(dyn chain::Listen + Send + Sync),
            ),
        ];

        for (blockhash, channel_monitor) in channelmonitors.drain(..) {
            let outpoint = channel_monitor.get_funding_txo().0;
            chain_listener_channel_monitors.push((
                blockhash,
                (
                    channel_monitor,
                    broadcaster.clone(),
                    fee_estimator.clone(),
                    logger.clone(),
                ),
                outpoint,
            ));
        }

        for monitor_listener_info in chain_listener_channel_monitors.iter_mut() {
            chain_listeners.push((
                monitor_listener_info.0,
                &monitor_listener_info.1 as &(dyn chain::Listen + Send + Sync),
            ));
        }

        init::synchronize_listeners(
            bitcoind_client.as_ref(),
            network,
            &mut cache,
            chain_listeners,
        )
        .await
        .unwrap()
    } else {
        polled_chain_tip
    };

    // Give ChannelMonitors to ChainMonitor
    for item in chain_listener_channel_monitors.drain(..) {
        let channel_monitor = item.1 .0;
        let funding_outpoint = item.2;
        assert_eq!(
            chain_monitor.watch_channel(funding_outpoint, channel_monitor),
            Ok(ChannelMonitorUpdateStatus::Completed)
        );
    }

    // Optional: Initialize the P2PGossipSync
    let gossip_sync = Arc::new(P2PGossipSync::new(
        Arc::clone(&network_graph),
        None,
        Arc::clone(&logger),
    ));

    // Initialize the PeerManager
    let channel_manager: Arc<ChannelManager> = Arc::new(channel_manager);
    let onion_messenger: Arc<OnionMessenger> = Arc::new(OnionMessenger::new(
        Arc::clone(&keys_manager),
        Arc::clone(&keys_manager),
        Arc::clone(&logger),
        Arc::clone(&channel_manager),
        Arc::new(DefaultMessageRouter::new(
            Arc::clone(&network_graph),
            Arc::clone(&keys_manager),
        )),
        Arc::clone(&channel_manager),
        IgnoringMessageHandler {},
    ));
    let mut ephemeral_bytes = [0; 32];
    let current_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    rand::thread_rng().fill_bytes(&mut ephemeral_bytes);
    let lightning_msg_handler = MessageHandler {
        chan_handler: channel_manager.clone(),
        route_handler: gossip_sync.clone(),
        onion_message_handler: onion_messenger.clone(),
        custom_message_handler: IgnoringMessageHandler {},
    };
    let peer_manager: Arc<PeerManager> = Arc::new(PeerManager::new(
        lightning_msg_handler,
        current_time.try_into().unwrap(),
        &ephemeral_bytes,
        logger.clone(),
        Arc::clone(&keys_manager),
    ));

    // Install a GossipVerifier in in the P2PGossipSync
    let utxo_lookup = GossipVerifier::new(
        Arc::clone(&bitcoind_client.bitcoind_rpc_client),
        lightning_block_sync::gossip::TokioSpawner,
        Arc::clone(&gossip_sync),
        Arc::clone(&peer_manager),
    );
    gossip_sync.add_utxo_lookup(Some(utxo_lookup));

    // ## Running LDK
    // Initialize networking

    let peer_manager_connection_handler = peer_manager.clone();
    let listening_port = ldk_peer_listening_port;
    let stop_processing = Arc::new(AtomicBool::new(false));
    let stop_listen = Arc::clone(&stop_processing);
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(format!("[::]:{}", listening_port))
            .await
            .expect("Failed to bind to listen port - is something else already listening on it?");
        loop {
            let peer_mgr = peer_manager_connection_handler.clone();
            let tcp_stream = listener.accept().await.unwrap().0;
            if stop_listen.load(Ordering::Acquire) {
                return;
            }
            tokio::spawn(async move {
                lightning_net_tokio::setup_inbound(
                    peer_mgr.clone(),
                    tcp_stream.into_std().unwrap(),
                )
                .await;
            });
        }
    });

    // Connect and Disconnect Blocks
    let output_sweeper: Arc<OutputSweeper> = Arc::new(output_sweeper);
    let channel_manager_listener = channel_manager.clone();
    let chain_monitor_listener = chain_monitor.clone();
    let output_sweeper_listener = output_sweeper.clone();
    let bitcoind_block_source = bitcoind_client.clone();
    let stop_listen = Arc::clone(&stop_processing);
    tokio::spawn(async move {
        let chain_poller = poll::ChainPoller::new(bitcoind_block_source.as_ref(), network);
        let chain_listener = (
            chain_monitor_listener,
            &(channel_manager_listener, output_sweeper_listener),
        );
        let mut spv_client = SpvClient::new(chain_tip, chain_poller, &mut cache, &chain_listener);
        loop {
            if stop_listen.load(Ordering::Acquire) {
                return;
            }
            spv_client.poll_best_tip().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    let inbound_payments = Arc::new(Mutex::new(disk::read_inbound_payment_info(
        &color_source.join(INBOUND_PAYMENTS_FNAME),
    )));
    let outbound_payments = Arc::new(Mutex::new(disk::read_outbound_payment_info(
        &color_source.join(OUTBOUND_PAYMENTS_FNAME),
    )));

    let bump_tx_event_handler = Arc::new(BumpTransactionEventHandler::new(
        Arc::clone(&broadcaster),
        Arc::new(Wallet::new(rgb_wallet_wrapper.clone(), Arc::clone(&logger))),
        Arc::clone(&keys_manager),
        Arc::clone(&logger),
    ));

    // Persist ChannelManager and NetworkGraph
    let persister = Arc::new(FilesystemStore::new(color_source_path.clone()));

    // Read swaps info
    let maker_swaps = Arc::new(Mutex::new(disk::read_swaps_info(
        &color_source.join(MAKER_SWAPS_FNAME),
    )));
    let taker_swaps = Arc::new(Mutex::new(disk::read_swaps_info(
        &color_source.join(TAKER_SWAPS_FNAME),
    )));

    // Read channel IDs info
    let channel_ids_map = Arc::new(Mutex::new(disk::read_channel_ids_info(
        &color_source.join(CHANNEL_IDS_FNAME),
    )));

    let unlocked_state = Arc::new(UnlockedAppState {
        channel_manager: Arc::clone(&channel_manager),
        inbound_payments,
        keys_manager,
        network_graph,
        onion_messenger,
        outbound_payments,
        peer_manager: Arc::clone(&peer_manager),
        fs_store: Arc::clone(&fs_store),
        bump_tx_event_handler,
        rgb_wallet_wrapper,
        maker_swaps,
        taker_swaps,
        router: Arc::clone(&router),
        output_sweeper: Arc::clone(&output_sweeper),
        rgb_send_lock: Arc::new(Mutex::new(false)),
        channel_ids_map,
    });

    let recent_payments_payment_ids = channel_manager
        .list_recent_payments()
        .into_iter()
        .map(|p| match p {
            RecentPaymentDetails::Pending { payment_id, .. } => payment_id,
            RecentPaymentDetails::Fulfilled { payment_id, .. } => payment_id,
            RecentPaymentDetails::Abandoned { payment_id, .. } => payment_id,
            RecentPaymentDetails::AwaitingInvoice { payment_id } => payment_id,
        })
        .collect::<Vec<PaymentId>>();
    unlocked_state.fail_outbound_pending_payments(recent_payments_payment_ids);

    // Handle LDK Events
    let unlocked_state_copy = Arc::clone(&unlocked_state);
    let static_state_copy = Arc::clone(static_state);
    let event_handler = move |event: Event| {
        let unlocked_state_copy = Arc::clone(&unlocked_state_copy);
        let static_state_copy = Arc::clone(&static_state_copy);
        async move {
            handle_ldk_events(event, unlocked_state_copy, static_state_copy).await;
        }
    };

    // Background Processing
    let (bp_exit, bp_exit_check) = tokio::sync::watch::channel(());
    let background_processor = tokio::spawn(process_events_async(
        persister,
        event_handler,
        chain_monitor.clone(),
        channel_manager.clone(),
        GossipSync::p2p(gossip_sync),
        peer_manager.clone(),
        logger.clone(),
        Some(scorer.clone()),
        move |t| {
            let mut bp_exit_fut_check = bp_exit_check.clone();
            Box::pin(async move {
                tokio::select! {
                    _ = tokio::time::sleep(t) => false,
                    _ = bp_exit_fut_check.changed() => true,
                }
            })
        },
        false,
        || {
            Some(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap(),
            )
        },
    ));

    // Regularly reconnect to channel peers.
    let connect_cm = Arc::clone(&channel_manager);
    let connect_pm = Arc::clone(&peer_manager);
    let peer_data_path = color_source.join(CHANNEL_PEER_DATA);
    let stop_connect = Arc::clone(&stop_processing);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            match disk::read_channel_peer_data(&peer_data_path) {
                Ok(info) => {
                    for node_id in connect_cm
                        .list_channels()
                        .iter()
                        .map(|chan| chan.counterparty.node_id)
                        .filter(|id| connect_pm.peer_by_node_id(id).is_none())
                    {
                        if stop_connect.load(Ordering::Acquire) {
                            return;
                        }
                        for (pubkey, peer_addr) in info.iter() {
                            if *pubkey == node_id {
                                let _ =
                                    do_connect_peer(*pubkey, *peer_addr, Arc::clone(&connect_pm))
                                        .await;
                            }
                        }
                    }
                }
                Err(e) => tracing::error!(
                    "ERROR: errored reading channel peer info from disk: {:?}",
                    e
                ),
            }
        }
    });

    // Regularly broadcast our node_announcement. This is only required (or possible) if we have
    // some public channels.
    let peer_man = Arc::clone(&peer_manager);
    let chan_man = Arc::clone(&channel_manager);
    tokio::spawn(async move {
        // First wait a minute until we have some peers and maybe have opened a channel.
        tokio::time::sleep(Duration::from_secs(60)).await;
        // Then, update our announcement once an hour to keep it fresh but avoid unnecessary churn
        // in the global gossip network.
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        loop {
            interval.tick().await;
            // Don't bother trying to announce if we don't have any public channls, though our
            // peers should drop such an announcement anyway. Note that announcement may not
            // propagate until we have a channel with 6+ confirmations.
            if chan_man.list_channels().iter().any(|chan| chan.is_public) {
                peer_man.broadcast_node_announcement(
                    [0; 3],
                    ldk_announced_node_name,
                    ldk_announced_listen_addr.clone(),
                );
            }
        }
    });

    tracing::info!("LDK logs are available at <your-supplied-ldk-data-dir-path>/.ldk/logs");
    tracing::info!("Local Node ID is {}", channel_manager.get_our_node_id());

    Ok((
        LdkBackgroundServices {
            stop_processing,
            peer_manager: peer_manager.clone(),
            bp_exit,
            background_processor: Some(background_processor),
        },
        unlocked_state,
    ))
}

impl AppState {
    fn stop_ldk(&self) -> Option<JoinHandle<Result<(), std::io::Error>>> {
        let mut ldk_background_services = self.get_ldk_background_services();

        if ldk_background_services.is_none() {
            // node is locked
            tracing::info!("LDK is not running");
            return None;
        }

        let ldk_background_services = ldk_background_services.as_mut().unwrap();

        // Disconnect our peers and stop accepting new connections. This ensures we don't continue
        // updating our channel data after we've stopped the background processor.
        ldk_background_services
            .stop_processing
            .store(true, Ordering::Release);
        ldk_background_services.peer_manager.disconnect_all_peers();

        // Stop the background processor.
        if !ldk_background_services.bp_exit.is_closed() {
            ldk_background_services.bp_exit.send(()).unwrap();
            ldk_background_services.background_processor.take()
        } else {
            None
        }
    }
}

pub(crate) async fn stop_ldk(app_state: Arc<AppState>) {
    tracing::info!("Stopping LDK");

    if let Some(join_handle) = app_state.stop_ldk() {
        join_handle.await.unwrap().unwrap();
    }

    // connect to the peer port so it can be released
    let peer_port = &app_state.static_state.ldk_peer_listening_port;
    let sock_addr = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        *peer_port,
    );
    let _ = std::net::TcpStream::connect(sock_addr);
    // check the peer port has been released
    let t_0 = OffsetDateTime::now_utc();
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if TcpListener::bind(sock_addr).is_ok() {
            break;
        }
        if (OffsetDateTime::now_utc() - t_0).as_seconds_f32() > 10.0 {
            panic!("LDK peer port not being released")
        }
    }

    tracing::info!("Stopped LDK");
}

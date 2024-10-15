use super::*;

const TEST_DIR_BASE: &str = "tmp/multi_open_close/";

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[traced_test]
async fn multi_open_close() {
    initialize();
    println!("Initialization complete");

    let test_dir_node1 = format!("{TEST_DIR_BASE}node1");
    println!("Test directory for node1: {}", test_dir_node1);
    
    let test_dir_node2 = format!("{TEST_DIR_BASE}node2");
    println!("Test directory for node2: {}", test_dir_node2);
    
    let test_dir_node3 = format!("{TEST_DIR_BASE}node3");
    println!("Test directory for node3: {}", test_dir_node3);
    
    let (node1_addr, _) = start_node(&test_dir_node1, NODE1_PEER_PORT, false).await;
    println!("Node1 started at address: {}", node1_addr);
    
    let (node2_addr, _) = start_node(&test_dir_node2, NODE2_PEER_PORT, false).await;
    println!("Node2 started at address: {}", node2_addr);
    
    let (node3_addr, _) = start_node(&test_dir_node3, NODE3_PEER_PORT, false).await;
    println!("Node3 started at address: {}", node3_addr);

    fund_and_create_utxos(node1_addr, None).await;
    println!("UTXOs funded and created for node1");
    
    fund_and_create_utxos(node2_addr, None).await;
    println!("UTXOs funded and created for node2");
    
    fund_and_create_utxos(node3_addr, None).await;
    println!("UTXOs funded and created for node3");

    let asset_id = issue_asset_nia(node1_addr).await.asset_id;
    println!("Asset issued with ID: {}", asset_id);

    let node2_pubkey = node_info(node2_addr).await.pubkey;
    println!("Node2 public key: {}", node2_pubkey);

    let channel = open_channel(
        node1_addr,
        &node2_pubkey,
        Some(NODE2_PEER_PORT),
        None,
        None,
        Some(600),
        Some(&asset_id),
    )
    .await;
    println!("Channel opened between node1 and node2");

    assert_eq!(asset_balance_spendable(node1_addr, &asset_id).await, 400);
    println!("Node1 spendable balance: 400");

    keysend(node1_addr, &node2_pubkey, None, Some(&asset_id), Some(100)).await;
    println!("Keysend 100 assets from node1 to node2");

    close_channel(node1_addr, &channel.channel_id, &node2_pubkey, false).await;
    println!("Channel closed between node1 and node2");

    wait_for_balance(node1_addr, &asset_id, 900).await;
    println!("Node1 balance after closing channel: 900");

    wait_for_balance(node2_addr, &asset_id, 100).await;
    println!("Node2 balance after closing channel: 100");

    let channel = open_channel(
        node1_addr,
        &node2_pubkey,
        Some(NODE2_PEER_PORT),
        None,
        None,
        Some(500),
        Some(&asset_id),
    )
    .await;
    println!("Channel reopened between node1 and node2");

    assert_eq!(asset_balance_spendable(node1_addr, &asset_id).await, 400);
    println!("Node1 spendable balance: 400");

    keysend(node1_addr, &node2_pubkey, None, Some(&asset_id), Some(100)).await;
    println!("Keysend 100 assets from node1 to node2");

    close_channel(node1_addr, &channel.channel_id, &node2_pubkey, false).await;
    println!("Channel closed between node1 and node2");

    wait_for_balance(node1_addr, &asset_id, 800).await;
    println!("Node1 balance after closing channel: 800");

    wait_for_balance(node2_addr, &asset_id, 200).await;
    println!("Node2 balance after closing channel: 200");

    let recipient_id = rgb_invoice(node3_addr, None).await.recipient_id;
    println!("Node3 recipient ID for invoice: {}", recipient_id);

    send_asset(node1_addr, &asset_id, 700, recipient_id).await;
    println!("Sent 700 assets from node1 to node3");

    mine(false);
    println!("Mining operation completed");

    refresh_transfers(node3_addr).await;
    println!("Transfers refreshed for node3");

    refresh_transfers(node3_addr).await;
    println!("Transfers refreshed for node3");

    refresh_transfers(node1_addr).await;
    println!("Transfers refreshed for node1");

    let recipient_id = rgb_invoice(node3_addr, None).await.recipient_id;
    println!("Node3 recipient ID for invoice: {}", recipient_id);

    send_asset(node2_addr, &asset_id, 150, recipient_id).await;
    println!("Sent 150 assets from node2 to node3");

    mine(false);
    println!("Mining operation completed");

    refresh_transfers(node3_addr).await;
    println!("Transfers refreshed for node3");

    refresh_transfers(node3_addr).await;
    println!("Transfers refreshed for node3");

    refresh_transfers(node2_addr).await;
    println!("Transfers refreshed for node2");

    assert_eq!(asset_balance_spendable(node1_addr, &asset_id).await, 100);
    println!("Node1 final spendable balance: 100");

    assert_eq!(asset_balance_spendable(node2_addr, &asset_id).await, 50);
    println!("Node2 final spendable balance: 50");

    assert_eq!(asset_balance_spendable(node3_addr, &asset_id).await, 850);
    println!("Node3 final spendable balance: 850");
}

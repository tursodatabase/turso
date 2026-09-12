use super::*;
use std::{sync::mpsc, time::Duration};

#[test]
fn vectored_write_is_not_observed_partially() {
    let store = Arc::new(MemStore::new());
    store.write_at(0, &[0x11; PAGE_SIZE]);
    let (write_entered, release_write) = store.pause_next_write();

    let writer_store = store.clone();
    let writer = std::thread::spawn(move || {
        writer_store.writev(
            0,
            &[
                Arc::new(Buffer::new(vec![0xAA; PAGE_SIZE / 2])),
                Arc::new(Buffer::new(vec![0xAA; PAGE_SIZE / 2])),
            ],
        );
    });
    write_entered.recv().unwrap();

    let (result_tx, result_rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let buffer = Buffer::new_temporary(PAGE_SIZE);
        store.read_into(0, &buffer);
        result_tx.send(buffer.as_slice().to_vec()).unwrap();
    });

    let early_result = result_rx.recv_timeout(Duration::from_secs(1));
    release_write.send(()).unwrap();
    writer.join().unwrap();
    let bytes = match early_result {
        Ok(bytes) => bytes,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            result_rx.recv_timeout(Duration::from_secs(5)).unwrap()
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("reader result channel disconnected")
        }
    };
    reader.join().unwrap();
    assert!(
        bytes.iter().all(|&byte| byte == 0xAA),
        "read returned a partially written file image; first old byte at offset {}",
        bytes.iter().position(|&byte| byte == 0x11).unwrap()
    );
}

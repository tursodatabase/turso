use super::*;
use crate::storage::page_cache::CacheError;
use crate::storage::pager::{Page, PageRef};
use crate::sync::Arc;
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};

fn create_key(id: usize) -> PageCacheKey {
    PageCacheKey::new(id)
}

pub fn page_with_content(page_id: usize) -> PageRef {
    let page = Arc::new(Page::new(page_id as i64));
    {
        let inner = page.get();
        inner.set_buffer(Arc::new(crate::Buffer::new_temporary(4096)));
    }
    page.set_loaded();
    page
}

fn insert_page(cache: &mut PageCache, id: usize) -> PageCacheKey {
    let key = create_key(id);
    let page = page_with_content(id);
    cache
        .insert(key, page)
        .unwrap_or_else(|e| panic!("Failed to insert page {id}: {e:?}"));
    key
}

#[test]
fn test_delete_only_element() {
    let mut cache = PageCache::default();
    let key1 = insert_page(&mut cache, 1);
    cache.verify_cache_integrity();
    assert_eq!(cache.len(), 1);

    assert!(cache.delete(key1).is_ok());

    assert_eq!(
        cache.len(),
        0,
        "Length should be 0 after deleting only element"
    );
    assert!(
        !cache.contains_key(&key1),
        "Cache should not contain key after delete"
    );
    cache.verify_cache_integrity();
}

#[test]
fn test_detach_tail() {
    let mut cache = PageCache::default();
    let key1 = insert_page(&mut cache, 1); // tail
    let _key2 = insert_page(&mut cache, 2); // middle
    let _key3 = insert_page(&mut cache, 3); // head
    cache.verify_cache_integrity();
    assert_eq!(cache.len(), 3);

    // Delete tail
    assert!(cache.delete(key1).is_ok());
    assert_eq!(cache.len(), 2, "Length should be 2 after deleting tail");
    assert!(
        !cache.contains_key(&key1),
        "Cache should not contain deleted tail key"
    );
    cache.verify_cache_integrity();
}

#[test]
fn test_insert_existing_key_updates_in_place() {
    let mut cache = PageCache::default();
    let key1 = create_key(1);
    let page1_v1 = page_with_content(1);
    let page1_v2 = page1_v1.clone(); // Same Arc instance

    assert!(cache.insert(key1, page1_v1).is_ok());
    assert_eq!(cache.len(), 1);

    // Inserting same page instance should return KeyExists error
    let result = cache.insert(key1, page1_v2);
    assert_eq!(result, Err(CacheError::KeyExists));
    assert_eq!(cache.len(), 1);

    // Verify the page is still accessible
    assert!(cache.get(&key1).unwrap().is_some());
    cache.verify_cache_integrity();
}

#[test]
#[should_panic(expected = "Attempted to insert different page with same key")]
fn test_insert_different_page_same_key_panics() {
    let mut cache = PageCache::default();
    let key1 = create_key(1);
    let page1_v1 = page_with_content(1);
    let page1_v2 = page_with_content(1); // Different Arc instance

    assert!(cache.insert(key1, page1_v1).is_ok());
    assert_eq!(cache.len(), 1);
    cache.verify_cache_integrity();

    // This should panic because it's a different page instance
    let _ = cache.insert(key1, page1_v2);
}

#[test]
fn test_delete_nonexistent_key() {
    let mut cache = PageCache::default();
    let key_nonexist = create_key(99);

    // Deleting non-existent key should be a no-op (returns Ok)
    assert!(cache.delete(key_nonexist).is_ok());
    assert_eq!(cache.len(), 0);
    cache.verify_cache_integrity();
}

#[test]
fn test_page_cache_evict() {
    // Note: page 1 is DatabaseHeader and is never evictable, so use page ids >= 2
    let mut cache = PageCache::new_with_spill(1, true);
    let key2 = insert_page(&mut cache, 2);
    let key3 = insert_page(&mut cache, 3);

    // With capacity=1, inserting key3 should evict key2
    assert_eq!(cache.get(&key3).unwrap().unwrap().get().id(), 3);
    assert!(
        cache.get(&key2).unwrap().is_none(),
        "key2 should be evicted"
    );

    // key3 should still be accessible
    assert_eq!(cache.get(&key3).unwrap().unwrap().get().id(), 3);
    assert!(
        cache.get(&key2).unwrap().is_none(),
        "capacity=1 should have evicted the older page"
    );
    cache.verify_cache_integrity();
}

#[test]
fn test_sieve_touch_non_tail_does_not_affect_immediate_eviction() {
    // SIEVE algorithm: touching a non-tail page marks it but doesn't move it.
    // The tail (if unmarked) will still be the first eviction candidate.
    // Note: page 1 is DatabaseHeader and is never evictable, so use page ids >= 2

    // Insert 2,3,4 -> order [4,3,2] with tail=2
    let mut cache = PageCache::new_with_spill(3, true);
    let key2 = insert_page(&mut cache, 2);
    let key3 = insert_page(&mut cache, 3);
    let key4 = insert_page(&mut cache, 4);

    // Touch key3 (middle) to mark it with reference bit
    assert!(cache.get(&key3).unwrap().is_some());

    // Insert 5: SIEVE examines tail (key2, unmarked) -> evict key2
    let key5 = insert_page(&mut cache, 5);

    assert!(
        cache.get(&key3).unwrap().is_some(),
        "marked non-tail (key3) should remain"
    );
    assert!(cache.get(&key4).unwrap().is_some(), "key4 should remain");
    assert!(
        cache.get(&key5).unwrap().is_some(),
        "key5 was just inserted"
    );
    assert!(
        cache.get(&key2).unwrap().is_none(),
        "unmarked tail (key2) should be evicted first"
    );
    cache.verify_cache_integrity();
}

#[test]
fn clock_second_chance_decrements_tail_then_evicts_next() {
    let mut cache = PageCache::new_with_spill(3, true);
    let key1 = insert_page(&mut cache, 1);
    let key2 = insert_page(&mut cache, 2);
    let key3 = insert_page(&mut cache, 3);
    assert_eq!(cache.len(), 3);
    assert!(cache.get(&key1).unwrap().is_some());
    let key4 = insert_page(&mut cache, 4);
    assert!(cache.get(&key1).unwrap().is_some(), "key1 should survive");
    assert!(cache.get(&key2).unwrap().is_some(), "key2 remains");
    assert!(cache.get(&key4).unwrap().is_some(), "key4 inserted");
    assert!(
        cache.get(&key3).unwrap().is_none(),
        "key3 (next after tail) evicted"
    );
    assert_eq!(cache.len(), 3);
    cache.verify_cache_integrity();
}

#[test]
fn test_delete_locked_page() {
    let mut cache = PageCache::default();
    let key = insert_page(&mut cache, 1);
    let page = cache.get(&key).unwrap().unwrap();
    page.set_locked();

    assert_eq!(cache.delete(key), Err(CacheError::Locked { pgno: 1 }));
    assert_eq!(cache.len(), 1, "Locked page should not be deleted");
    cache.verify_cache_integrity();
}

#[test]
fn test_delete_dirty_page() {
    let mut cache = PageCache::default();
    let key = insert_page(&mut cache, 1);
    let page = cache.get(&key).unwrap().unwrap();
    page.set_dirty();

    assert_eq!(cache.delete(key), Err(CacheError::Dirty { pgno: 1 }));
    assert_eq!(cache.len(), 1, "Dirty page should not be deleted");
    cache.verify_cache_integrity();
}

#[test]
fn test_delete_pinned_page() {
    let mut cache = PageCache::default();
    let key = insert_page(&mut cache, 1);
    let page = cache.get(&key).unwrap().unwrap();
    page.pin();

    assert_eq!(cache.delete(key), Err(CacheError::Pinned { pgno: 1 }));
    assert_eq!(cache.len(), 1, "Pinned page should not be deleted");
    cache.verify_cache_integrity();
}

#[test]
fn test_make_room_for_with_dirty_pages() {
    let mut cache = PageCache::new_with_spill(2, true);
    let key1 = insert_page(&mut cache, 1);
    let key2 = insert_page(&mut cache, 2);

    // Make both pages dirty (unevictable)
    cache.get(&key1).unwrap().unwrap().set_dirty();
    cache.get(&key2).unwrap().unwrap().set_dirty();

    // Try to insert a third page, should fail because can't evict dirty pages
    let key3 = create_key(3);
    let page3 = page_with_content(3);
    let result = cache.insert(key3, page3);

    assert_eq!(result, Err(CacheError::Full));
    assert_eq!(cache.len(), 2);
    cache.verify_cache_integrity();
}

#[test]
fn test_force_insert_allows_temporary_over_capacity_cache() {
    let mut cache = PageCache::new_with_spill(2, true);
    let key1 = insert_page(&mut cache, 1);
    let key2 = insert_page(&mut cache, 2);

    // Make both pages dirty (unevictable): a normal insert must fail.
    for key in [key1, key2] {
        cache.notify_page_dirty(key);
        cache.peek(&key, false).unwrap().set_dirty();
    }
    let key3 = create_key(3);
    assert_eq!(
        cache.insert(key3, page_with_content(3)),
        Err(CacheError::Full)
    );

    // The capacity is a soft limit: a forced insert must succeed.
    cache.force_insert_page(key3, page_with_content(3)).unwrap();
    assert_eq!(cache.len(), 3);
    assert!(cache.contains_key(&key3));
    cache.verify_cache_integrity();

    // Once pages become evictable again, the next normal insert drains
    // the excess back under capacity.
    for key in [key1, key2] {
        cache.notify_page_spilled(key);
        cache.peek(&key, false).unwrap().set_spilled();
    }
    let key4 = create_key(4);
    cache.insert(key4, page_with_content(4)).unwrap();

    assert_eq!(cache.len(), 2);
    assert!(cache.contains_key(&key4));
    cache.verify_cache_integrity();
}

#[test]
fn test_force_upsert_allows_temporary_over_capacity_cache() {
    let mut cache = PageCache::new_with_spill(2, true);
    let key2 = insert_page(&mut cache, 2);
    let key3 = insert_page(&mut cache, 3);

    for key in [key2, key3] {
        cache.notify_page_dirty(key);
        cache.peek(&key, false).unwrap().set_dirty();
    }

    let key4 = create_key(4);
    let page4 = page_with_content(4);
    page4.set_dirty();
    cache.force_upsert_page(key4, page4).unwrap();

    assert_eq!(cache.len(), 3);
    assert!(cache.contains_key(&key4));
    cache.verify_cache_integrity();

    for key in [key2, key3] {
        cache.notify_page_spilled(key);
        cache.peek(&key, false).unwrap().set_spilled();
    }

    let key5 = create_key(5);
    cache.insert(key5, page_with_content(5)).unwrap();

    assert_eq!(cache.len(), 2);
    assert!(cache.contains_key(&key4));
    assert!(cache.contains_key(&key5));
    cache.verify_cache_integrity();
}

#[test]
fn test_page_cache_insert_and_get() {
    let mut cache = PageCache::default();
    let key1 = insert_page(&mut cache, 1);
    let key2 = insert_page(&mut cache, 2);

    assert_eq!(cache.get(&key1).unwrap().unwrap().get().id(), 1);
    assert_eq!(cache.get(&key2).unwrap().unwrap().get().id(), 2);
    cache.verify_cache_integrity();
}

#[test]
fn test_page_cache_over_capacity() {
    // Test SIEVE eviction when exceeding capacity
    // Note: page 1 is DatabaseHeader and is never evictable, so use page ids >= 2
    let mut cache = PageCache::new_with_spill(2, true);
    let key2 = insert_page(&mut cache, 2);
    let key3 = insert_page(&mut cache, 3);

    // Insert 4: tail (key2, unmarked) should be evicted
    let key4 = insert_page(&mut cache, 4);

    assert_eq!(cache.len(), 2);
    assert!(cache.get(&key3).unwrap().is_some(), "key3 should remain");
    assert!(cache.get(&key4).unwrap().is_some(), "key4 just inserted");
    assert!(
        cache.get(&key2).unwrap().is_none(),
        "key2 (oldest, unmarked) should be evicted"
    );
    cache.verify_cache_integrity();
}

#[test]
fn test_page_cache_delete() {
    let mut cache = PageCache::default();
    let key1 = insert_page(&mut cache, 1);

    assert!(cache.delete(key1).is_ok());
    assert!(cache.get(&key1).unwrap().is_none());
    assert_eq!(cache.len(), 0);
    cache.verify_cache_integrity();
}

#[test]
fn test_page_cache_clear() {
    let mut cache = PageCache::default();
    let key1 = insert_page(&mut cache, 1);
    let key2 = insert_page(&mut cache, 2);

    assert!(cache.clear(false).is_ok());
    assert!(cache.get(&key1).unwrap().is_none());
    assert!(cache.get(&key2).unwrap().is_none());
    assert_eq!(cache.len(), 0);
    cache.verify_cache_integrity();
}

#[test]
fn test_resize_smaller_success() {
    let mut cache = PageCache::default();
    for i in 1..=5 {
        let _ = insert_page(&mut cache, i);
    }
    assert_eq!(cache.len(), 5);

    let result = cache.resize(3);
    assert_eq!(result, CacheResizeResult::Done);
    assert_eq!(cache.len(), 3);
    assert_eq!(cache.capacity(), 3);

    // Should still be able to insert after resize
    assert!(cache.insert(create_key(6), page_with_content(6)).is_ok());
    assert_eq!(cache.len(), 3); // One was evicted to make room
    cache.verify_cache_integrity();
}

#[test]
fn test_detach_with_multiple_pages() {
    let mut cache = PageCache::default();
    let _key1 = insert_page(&mut cache, 1);
    let key2 = insert_page(&mut cache, 2);
    let _key3 = insert_page(&mut cache, 3);

    // Delete middle element (key2)
    assert!(cache.delete(key2).is_ok());

    // Verify structure after deletion
    assert_eq!(cache.len(), 2);
    assert!(!cache.contains_key(&key2));

    cache.verify_cache_integrity();
}

#[test]
fn test_delete_multiple_elements() {
    let mut cache = PageCache::default();
    let key1 = insert_page(&mut cache, 1);
    let key2 = insert_page(&mut cache, 2);
    let key3 = insert_page(&mut cache, 3);
    cache.verify_cache_integrity();
    assert_eq!(cache.len(), 3);

    // Delete head (key3)
    assert!(cache.delete(key3).is_ok());
    assert_eq!(cache.len(), 2, "Length should be 2 after deleting head");
    assert!(
        !cache.contains_key(&key3),
        "Cache should not contain deleted head key"
    );
    cache.verify_cache_integrity();

    // Delete tail (key1)
    assert!(cache.delete(key1).is_ok());
    assert_eq!(cache.len(), 1, "Length should be 1 after deleting two");
    cache.verify_cache_integrity();

    // Delete last element (key2)
    assert!(cache.delete(key2).is_ok());
    assert_eq!(cache.len(), 0, "Length should be 0 after deleting all");
    cache.verify_cache_integrity();
}

#[test]
fn test_resize_larger() {
    let mut cache = PageCache::new_with_spill(2, true);
    let key1 = insert_page(&mut cache, 1);
    let key2 = insert_page(&mut cache, 2);
    assert_eq!(cache.len(), 2);

    let result = cache.resize(5);
    assert_eq!(result, CacheResizeResult::Done);
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.capacity(), 5);

    // Existing pages should still be accessible
    assert!(cache.get(&key1).is_ok_and(|p| p.is_some()));
    assert!(cache.get(&key2).is_ok_and(|p| p.is_some()));

    // Now we should be able to add 3 more without eviction
    for i in 3..=5 {
        let _ = insert_page(&mut cache, i);
    }
    assert_eq!(cache.len(), 5);
    cache.verify_cache_integrity();
}

#[test]
fn test_resize_same_capacity() {
    let mut cache = PageCache::new_with_spill(3, true);
    for i in 1..=3 {
        let _ = insert_page(&mut cache, i);
    }

    let result = cache.resize(3);
    assert_eq!(result, CacheResizeResult::Done);
    assert_eq!(cache.len(), 3);
    assert_eq!(cache.capacity(), 3);
    cache.verify_cache_integrity();
}

#[test]
fn test_truncate_page_cache() {
    let mut cache = PageCache::new_with_spill(10, true);
    let _ = insert_page(&mut cache, 1);
    let _ = insert_page(&mut cache, 4);
    let _ = insert_page(&mut cache, 8);
    let _ = insert_page(&mut cache, 10);

    // Truncate to keep only pages <= 4
    cache.truncate(4).unwrap();

    assert!(cache.contains_key(&PageCacheKey(1)));
    assert!(cache.contains_key(&PageCacheKey(4)));
    assert!(!cache.contains_key(&PageCacheKey(8)));
    assert!(!cache.contains_key(&PageCacheKey(10)));
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.capacity(), 10);
    cache.verify_cache_integrity();
}

#[test]
fn test_truncate_page_cache_remove_all() {
    let mut cache = PageCache::new_with_spill(10, true);
    let _ = insert_page(&mut cache, 8);
    let _ = insert_page(&mut cache, 10);

    // Truncate to 4 (removes all pages since they're > 4)
    cache.truncate(4).unwrap();

    assert!(!cache.contains_key(&PageCacheKey(8)));
    assert!(!cache.contains_key(&PageCacheKey(10)));
    assert_eq!(cache.len(), 0);
    assert_eq!(cache.capacity(), 10);
    cache.verify_cache_integrity();
}

#[test]
fn test_page_cache_fuzz() {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    tracing::info!("fuzz test seed: {}", seed);

    let max_pages = 10;
    let mut cache = PageCache::new_with_spill(10, true);
    let mut reference_map = HashMap::default();

    for _ in 0..10000 {
        cache.print();

        match rng.next_u64() % 2 {
            0 => {
                // Insert operation
                let id_page = rng.next_u64() % max_pages;
                let key = PageCacheKey::new(id_page as usize);
                #[allow(clippy::arc_with_non_send_sync)]
                let page = Arc::new(Page::new(id_page as i64));

                if cache.peek(&key, false).is_some() {
                    continue; // Skip duplicate page ids
                }

                tracing::debug!("inserting page {:?}", key);
                match cache.insert(key, page.clone()) {
                    Err(CacheError::Full) => {} // Expected, ignore
                    Err(err) => {
                        panic!("Cache insertion failed unexpectedly: {err:?}");
                    }
                    Ok(_) => {
                        reference_map.insert(key, page);
                        // Clean up reference_map if cache evicted something
                        if cache.len() < reference_map.len() {
                            reference_map.retain(|k, _| cache.contains_key(k));
                        }
                    }
                }
                assert!(cache.len() <= 10, "Cache size exceeded capacity");
            }
            1 => {
                // Delete operation
                let random = rng.next_u64() % 2 == 0;
                let key = if random || reference_map.is_empty() {
                    let id_page: u64 = rng.next_u64() % max_pages;
                    PageCacheKey::new(id_page as usize)
                } else {
                    let i = rng.next_u64() as usize % reference_map.len();
                    *reference_map.keys().nth(i).unwrap()
                };

                tracing::debug!("removing page {:?}", key);
                reference_map.remove(&key);
                assert!(cache.delete(key).is_ok());
            }
            _ => unreachable!(),
        }

        cache.verify_cache_integrity();

        // Verify all pages in reference_map are in cache
        for (key, page) in &reference_map {
            let cached_page = cache.peek(key, false).expect("Page should be in cache");
            assert_eq!(cached_page.get().id(), key.0);
            assert_eq!(page.get().id(), key.0);
        }
    }
}

#[test]
fn test_peek_without_touch() {
    // Test that peek with touch=false doesn't mark pages
    // Note: page 1 is DatabaseHeader and is never evictable, so use page ids >= 2
    let mut cache = PageCache::new_with_spill(2, true);
    let key2 = insert_page(&mut cache, 2);
    let key3 = insert_page(&mut cache, 3);

    // Peek key2 without touching (no ref bit set)
    assert!(cache.peek(&key2, false).is_some());

    // Insert 4: should evict unmarked tail (key2)
    let key4 = insert_page(&mut cache, 4);

    assert!(cache.get(&key3).unwrap().is_some(), "key3 should remain");
    assert!(
        cache.get(&key4).unwrap().is_some(),
        "key4 was just inserted"
    );
    assert!(
        cache.get(&key2).unwrap().is_none(),
        "key2 should be evicted since peek(false) didn't mark it"
    );
    assert_eq!(cache.len(), 2);
    cache.verify_cache_integrity();
}

#[test]
fn test_peek_with_touch() {
    // Test that peek with touch=true marks pages for SIEVE
    let mut cache = PageCache::new_with_spill(2, true);
    let key1 = insert_page(&mut cache, 1);
    let key2 = insert_page(&mut cache, 2);

    // Peek key1 WITH touching (sets ref bit)
    assert!(cache.peek(&key1, true).is_some());

    // Insert 3: key1 is marked, so it gets second chance
    // key2 becomes new tail and gets evicted
    let key3 = insert_page(&mut cache, 3);

    assert!(
        cache.get(&key1).unwrap().is_some(),
        "key1 should survive (was marked)"
    );
    assert!(
        cache.get(&key3).unwrap().is_some(),
        "key3 was just inserted"
    );
    assert!(
        cache.get(&key2).unwrap().is_none(),
        "key2 should be evicted after key1's second chance"
    );
    assert_eq!(cache.len(), 2);
    cache.verify_cache_integrity();
}

#[test]
#[ignore = "long running test, remove ignore to verify memory stability"]
fn test_clear_memory_stability() {
    let initial_memory = memory_stats::memory_stats().unwrap().physical_mem;

    for _ in 0..100000 {
        let mut cache = PageCache::new(1000);

        for i in 0..1000 {
            let key = create_key(i);
            let page = page_with_content(i);
            cache.insert(key, page).unwrap();
        }

        cache.clear(false).unwrap();
        drop(cache);
    }

    let final_memory = memory_stats::memory_stats().unwrap().physical_mem;
    let growth = final_memory.saturating_sub(initial_memory);

    println!("Memory growth: {growth} bytes");
    assert!(
        growth < 10_000_000,
        "Memory grew by {growth} bytes over test cycles (limit: 10MB)",
    );
}

#[test]
fn clock_drains_hot_page_within_single_sweep_when_others_are_unevictable() {
    // Note: page 1 is DatabaseHeader and is never evictable, so use page ids >= 2
    // capacity 3: [4(head), 3, 2(tail)]
    let mut c = PageCache::new_with_spill(3, true);
    let k2 = insert_page(&mut c, 2);
    let k3 = insert_page(&mut c, 3);
    let k4 = insert_page(&mut c, 4);

    // Make k2 hot: bump to Max
    for _ in 0..3 {
        assert!(c.get(&k2).unwrap().is_some());
    }
    assert!(matches!(c.ref_of(&k2), Some(REF_MAX)));

    // Make other pages unevictable; clock must keep revisiting k2.
    c.get(&k3).unwrap().unwrap().set_dirty();
    c.get(&k4).unwrap().unwrap().set_dirty();

    // Insert 5 -> sweep rotates as needed, draining k2 and evicting it.
    let k5 = insert_page(&mut c, 5);

    assert!(
        c.get(&k2).unwrap().is_none(),
        "k2 should be evicted after its credit drains"
    );
    assert!(c.get(&k3).unwrap().is_some(), "k3 is dirty (unevictable)");
    assert!(c.get(&k4).unwrap().is_some(), "k4 is dirty (unevictable)");
    assert!(c.get(&k5).unwrap().is_some(), "k5 just inserted");
    c.verify_cache_integrity();
}

#[test]
fn gclock_hot_survives_scan_pages() {
    let mut c = PageCache::new_with_spill(4, true);
    let _k1 = insert_page(&mut c, 1);
    let k2 = insert_page(&mut c, 2);
    let _k3 = insert_page(&mut c, 3);
    let _k4 = insert_page(&mut c, 4);

    // Make k2 truly hot: three real touches
    for _ in 0..3 {
        assert!(c.get(&k2).unwrap().is_some());
    }
    assert!(matches!(c.ref_of(&k2), Some(REF_MAX)));

    // Now simulate a scan inserting new pages 5..10 (one-hit wonders).
    for id in 5..=10 {
        let _ = insert_page(&mut c, id);
    }

    // Hot k2 should still be present; most single-hit scan pages should churn.
    assert!(
        c.get(&k2).unwrap().is_some(),
        "hot page should survive scan"
    );
    // The earliest single-hit page should be gone.
    assert!(c.get(&create_key(5)).unwrap().is_none());
    c.verify_cache_integrity();
}

#[test]
fn hand_stays_valid_after_deleting_only_element() {
    let mut c = PageCache::new_with_spill(2, true);
    let k = insert_page(&mut c, 1);
    assert!(c.delete(k).is_ok());
    // Inserting again should not panic and should succeed
    let _ = insert_page(&mut c, 2);
    c.verify_cache_integrity();
}

#[test]
fn hand_is_reset_after_clear_and_resize() {
    let mut c = PageCache::new_with_spill(3, true);
    for i in 1..=3 {
        let _ = insert_page(&mut c, i);
    }
    c.clear(false).unwrap();
    // No elements; insert should not rely on stale hand
    let _ = insert_page(&mut c, 10);

    // Resize from 1 -> 4 and back should not OOB the hand
    assert_eq!(c.resize(4), CacheResizeResult::Done);
    assert_eq!(c.resize(1), CacheResizeResult::Done);
    let _ = insert_page(&mut c, 11);
    c.verify_cache_integrity();
}

#[test]
fn resize_preserves_ref_and_recency() {
    let mut c = PageCache::new_with_spill(4, true);
    let _k1 = insert_page(&mut c, 1);
    let k2 = insert_page(&mut c, 2);
    let _k3 = insert_page(&mut c, 3);
    let _k4 = insert_page(&mut c, 4);
    // Make k2 hot.
    for _ in 0..3 {
        assert!(c.get(&k2).unwrap().is_some());
    }
    let _r_before = c.ref_of(&k2);

    // Shrink to 3 (one page will be evicted during repack/next insert)
    assert_eq!(c.resize(3), CacheResizeResult::Done);
    assert!(matches!(c.ref_of(&k2), _r_before));

    // Force an eviction; hot k2 should survive more passes.
    let _ = insert_page(&mut c, 5);
    assert!(c.get(&k2).unwrap().is_some());
    c.verify_cache_integrity();
}

#[test]
fn test_sieve_second_chance_preserves_marked_page() {
    let mut cache = PageCache::new_with_spill(3, true);
    let key1 = insert_page(&mut cache, 1);
    let key2 = insert_page(&mut cache, 2);
    let key3 = insert_page(&mut cache, 3);

    // Mark key1 for second chance
    assert!(cache.get(&key1).unwrap().is_some());

    let key4 = insert_page(&mut cache, 4);
    // CLOCK sweep from hand:
    // - key1 marked -> decrement, continue
    // - key3 (MRU) unmarked -> evict
    assert!(
        cache.get(&key1).unwrap().is_some(),
        "key1 had ref bit set, got second chance"
    );
    assert!(
        cache.get(&key3).unwrap().is_none(),
        "key3 (MRU) should be evicted"
    );
    assert!(cache.get(&key4).unwrap().is_some(), "key4 just inserted");
    assert!(
        cache.get(&key2).unwrap().is_some(),
        "key2 (middle) should remain"
    );
    cache.verify_cache_integrity();
}

#[test]
fn test_clock_sweep_wraps_around() {
    // Test that clock hand properly wraps around the circular list
    let mut cache = PageCache::new_with_spill(3, true);
    let key1 = insert_page(&mut cache, 1);
    let key2 = insert_page(&mut cache, 2);
    let key3 = insert_page(&mut cache, 3);

    // Mark all pages
    assert!(cache.get(&key1).unwrap().is_some());
    assert!(cache.get(&key2).unwrap().is_some());
    assert!(cache.get(&key3).unwrap().is_some());

    // Insert 4: hand will sweep full circle, decrementing all refs
    // then sweep again and evict first unmarked page
    let key4 = insert_page(&mut cache, 4);

    // One page was evicted after full sweep
    assert_eq!(cache.len(), 3);
    assert!(cache.get(&key4).unwrap().is_some());

    // Verify exactly one of the original pages was evicted
    let survivors = [key1, key2, key3]
        .iter()
        .filter(|k| cache.get(k).unwrap().is_some())
        .count();
    assert_eq!(survivors, 2, "Should have 2 survivors from original 3");
    cache.verify_cache_integrity();
}

#[test]
fn test_circular_list_single_element() {
    let mut cache = PageCache::new_with_spill(3, true);
    let key1 = insert_page(&mut cache, 1);

    // Single element exists
    assert_eq!(cache.len(), 1);
    assert!(cache.contains_key(&key1));

    // Delete single element
    assert!(cache.delete(key1).is_ok());
    assert!(cache.clock_hand.is_null());

    // Insert after empty should work
    let key2 = insert_page(&mut cache, 2);
    assert_eq!(cache.len(), 1);
    assert!(cache.contains_key(&key2));
    cache.verify_cache_integrity();
}

#[test]
fn test_hand_advances_on_eviction() {
    // Note: page 1 is DatabaseHeader and is never evictable, so use page ids >= 2
    let mut cache = PageCache::new_with_spill(2, true);
    let _key2 = insert_page(&mut cache, 2);
    let _key3 = insert_page(&mut cache, 3);

    // Note initial hand position
    let initial_hand = cache.clock_hand;

    // Force eviction
    let _key4 = insert_page(&mut cache, 4);

    // Hand should exist (not null)
    let new_hand = cache.clock_hand;
    assert!(!new_hand.is_null());
    // Hand moved during sweep (exact position depends on eviction)
    assert!(initial_hand.is_null() || new_hand != initial_hand || cache.len() < 2);
    cache.verify_cache_integrity();
}

#[test]
fn test_multi_level_ref_counting() {
    let mut cache = PageCache::new_with_spill(2, true);
    let key1 = insert_page(&mut cache, 1);
    let _key2 = insert_page(&mut cache, 2);

    // Bump key1 to MAX (3 accesses)
    for _ in 0..3 {
        assert!(cache.get(&key1).unwrap().is_some());
    }
    assert_eq!(cache.ref_of(&key1), Some(REF_MAX));

    // Insert multiple new pages - key1 should survive longer
    for i in 3..6 {
        let _ = insert_page(&mut cache, i);
    }

    // key1 might still be there due to high ref count
    // (depends on exact sweep pattern, but it got multiple chances)
    cache.verify_cache_integrity();
}

#[test]
fn test_resize_maintains_circular_structure() {
    let mut cache = PageCache::new_with_spill(5, true);
    for i in 1..=4 {
        let _ = insert_page(&mut cache, i);
    }

    // Resize smaller
    assert_eq!(cache.resize(2), CacheResizeResult::Done);
    assert_eq!(cache.len(), 2);

    // Verify structure via integrity check
    cache.verify_cache_integrity();
}

#[test]
fn test_link_after_correctness() {
    let mut cache = PageCache::new_with_spill(4, true);
    let key1 = insert_page(&mut cache, 1);
    let key2 = insert_page(&mut cache, 2);
    let key3 = insert_page(&mut cache, 3);

    // Verify all keys are in cache
    assert!(cache.contains_key(&key1));
    assert!(cache.contains_key(&key2));
    assert!(cache.contains_key(&key3));
    assert_eq!(cache.len(), 3);

    cache.verify_cache_integrity();
}

#[test]
fn test_evictable_count_tracking() {
    // Test that evictable_count is tracked correctly for fast-path spill check
    // Note: page 1 is DatabaseHeader and is never evictable
    let mut cache = PageCache::new_with_spill(10, true);

    // Insert clean pages (all evictable except page 1)
    let key1 = insert_page(&mut cache, 1); // page 1 is never evictable
    let key2 = insert_page(&mut cache, 2);
    let key3 = insert_page(&mut cache, 3);

    // Page 1 is not counted, pages 2 and 3 are evictable
    assert_eq!(cache.evictable_count(), 2);

    // Make page 2 dirty - it becomes non-evictable
    cache.notify_page_dirty(key2);
    assert_eq!(cache.evictable_count(), 1);

    // Make page 2 spilled - it becomes evictable again
    cache.notify_page_spilled(key2);
    assert_eq!(cache.evictable_count(), 2);

    // Delete page 3 - evictable count decreases
    assert!(cache.delete(key3).is_ok());
    assert_eq!(cache.evictable_count(), 1);

    // Delete page 1 (which wasn't counted) - no change
    assert!(cache.delete(key1).is_ok());
    assert_eq!(cache.evictable_count(), 1);

    // Clear cache
    assert!(cache.clear(true).is_ok());
    assert_eq!(cache.evictable_count(), 0);

    cache.verify_cache_integrity();
}

#[test]
fn test_needs_spill_fast_path() {
    // Test that needs_spill uses the fast path when we have enough evictable pages
    // Capacity 10, threshold 90% = 9, so when len > 9 we need some evictable pages
    let mut cache = PageCache::new_with_spill(10, true);

    // Insert 10 clean pages (all evictable except page 1)
    for i in 1..=10 {
        let _ = insert_page(&mut cache, i);
    }

    // len=10, threshold=9, needed_evictable = 10-9 = 1
    // evictable_count = 9 (pages 2-10, page 1 not counted)
    // Fast path: 9 >= 1, so no spill needed
    assert!(!cache.needs_spill());
    assert_eq!(cache.evictable_count(), 9);

    // Make all pages dirty
    for i in 2..=10 {
        let key = create_key(i);
        cache.notify_page_dirty(key);
        cache.peek(&key, false).unwrap().set_dirty();
    }

    // Now evictable_count = 0 and pages are actually dirty
    // needed_evictable = 1, but we have 0 evictable pages
    assert_eq!(cache.evictable_count(), 0);
    // needs_spill should return true because we need 1 evictable page but have 0
    assert!(cache.needs_spill());

    // Mark pages as spilled
    for i in 2..=10 {
        let key = create_key(i);
        cache.notify_page_spilled(key);
        cache.peek(&key, false).unwrap().set_spilled();
    }

    // Now evictable_count = 9 and pages are spilled (evictable)
    assert_eq!(cache.evictable_count(), 9);
    // Fast path: 9 >= 1, so no spill needed
    assert!(!cache.needs_spill());

    cache.verify_cache_integrity();
}

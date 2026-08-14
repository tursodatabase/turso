//! Unit tests for policy trait implementations.

#[cfg(test)]
mod value_array_tests {
    use crate::policy::{
        BoxPolicy, BoxValueArray, InlineValueArray, LeafPolicy, RetireHandle, ValueArray, ValuePtr,
    };

    // ====================================================================
    //  BoxValueArray<V> Tests
    // ====================================================================

    #[test]
    fn box_new_all_empty() {
        let arr: BoxValueArray<u64> = BoxValueArray::new();
        for slot in 0..15 {
            assert!(arr.is_empty(slot));
            assert!(arr.load(slot).is_none());
        }
    }

    #[test]
    fn box_store_and_load() {
        let arr: BoxValueArray<u64> = BoxValueArray::new();
        let val: ValuePtr<u64> = BoxPolicy::into_output(42u64);

        arr.store(0, &val);

        assert!(!arr.is_empty(0));
        let loaded: ValuePtr<u64> = arr.load(0).unwrap();
        assert_eq!(*loaded, 42);

        // Cleanup to avoid leak.
        unsafe { arr.cleanup(0) };
    }

    #[test]
    fn box_store_relaxed() {
        let arr: BoxValueArray<u64> = BoxValueArray::new();
        let val: ValuePtr<u64> = BoxPolicy::into_output(99u64);

        arr.store_relaxed(3, &val);

        assert!(!arr.is_empty(3));
        let loaded: ValuePtr<u64> = arr.load(3).unwrap();
        assert_eq!(*loaded, 99);

        unsafe { arr.cleanup(3) };
    }

    #[test]
    fn box_update_in_place_returns_ptr_handle() {
        let arr: BoxValueArray<u64> = BoxValueArray::new();
        let old_val: ValuePtr<u64> = BoxPolicy::into_output(100u64);
        let new_val: ValuePtr<u64> = BoxPolicy::into_output(200u64);

        arr.store(5, &old_val);

        let handle: RetireHandle = arr.update_in_place(5, &new_val);

        // Handle must be Ptr (old Box needs retirement).
        match handle {
            RetireHandle::Ptr(ptr) => assert!(!ptr.is_null()),
            RetireHandle::Noop => panic!("expected Ptr handle for Box"),
        }

        let loaded: ValuePtr<u64> = arr.load(5).unwrap();
        assert_eq!(*loaded, 200);

        // Retire old value manually (in production, guard.defer_retire does this).
        if let RetireHandle::Ptr(ptr) = handle {
            unsafe { drop(Box::from_raw(ptr.cast::<u64>())) };
        }

        unsafe { arr.cleanup(5) };
    }

    #[test]
    fn box_take_returns_value_and_empties() {
        let arr: BoxValueArray<u64> = BoxValueArray::new();
        let val: ValuePtr<u64> = BoxPolicy::into_output(77u64);

        arr.store(7, &val);
        assert!(!arr.is_empty(7));

        let taken: ValuePtr<u64> = arr.take(7).unwrap();
        assert_eq!(*taken, 77);
        assert!(arr.is_empty(7));
        assert!(arr.take(7).is_none());

        // Must free the taken value (no refcount).
        unsafe { drop(Box::from_raw(taken.as_ptr())) };
    }

    #[test]
    fn box_clear_makes_empty() {
        let arr: BoxValueArray<u64> = BoxValueArray::new();
        let val: ValuePtr<u64> = BoxPolicy::into_output(55u64);

        arr.store(2, &val);
        assert!(!arr.is_empty(2));

        // NOTE: clear does NOT drop the Box — caller must handle retirement.
        // Retrieve raw pointer before clearing so we can drop the orphaned allocation.
        let raw = arr.load_raw(2);
        arr.clear(2);
        assert!(arr.is_empty(2));

        // SAFETY: raw was stored via Box::into_raw in into_output().
        unsafe { drop(Box::from_raw(raw.cast::<u64>())) };
    }

    #[test]
    fn box_move_slot_transfers_ownership() {
        let src: BoxValueArray<u64> = BoxValueArray::new();
        let dst: BoxValueArray<u64> = BoxValueArray::new();
        let val: ValuePtr<u64> = BoxPolicy::into_output(33u64);

        src.store(4, &val);

        // Move from src[4] to dst[9].
        src.move_slot(&dst, 4, 9);
        src.clear(4); // Required after move.

        assert!(src.is_empty(4));
        assert!(!dst.is_empty(9));

        let loaded: ValuePtr<u64> = dst.load(9).unwrap();
        assert_eq!(*loaded, 33);

        unsafe { dst.cleanup(9) };
    }

    #[test]
    fn box_layer_pointer_ops() {
        let arr: BoxValueArray<u64> = BoxValueArray::new();

        // Use a real allocation for provenance-safe layer pointer simulation.
        let fake_layer: *mut u8 = Box::into_raw(Box::new(0u8));

        arr.store_layer(10, fake_layer);
        assert!(!arr.is_empty(10));

        let loaded: *mut u8 = arr.load_layer(10);
        assert_eq!(loaded, fake_layer);

        arr.clear(10);
        assert!(arr.is_empty(10));

        // Reclaim the allocation.
        unsafe { drop(Box::from_raw(fake_layer)) };
    }

    // ====================================================================
    //  InlineValueArray<V> Tests
    // ====================================================================

    #[test]
    fn inline_new_all_empty() {
        let arr: InlineValueArray<u64> = InlineValueArray::new();
        for slot in 0..15 {
            assert!(arr.is_empty(slot));
            assert!(arr.load(slot).is_none());
            assert!(!arr.is_layer(slot));
        }
    }

    #[test]
    fn inline_store_and_load() {
        let arr: InlineValueArray<u64> = InlineValueArray::new();

        arr.store(0, &42u64);

        assert!(!arr.is_empty(0));
        assert!(!arr.is_layer(0));
        let loaded: u64 = arr.load(0).unwrap();
        assert_eq!(loaded, 42);
    }

    #[test]
    fn inline_store_zero_value() {
        // Regression: zero value must work (no XOR magic needed).
        let arr: InlineValueArray<u64> = InlineValueArray::new();

        arr.store(1, &0u64);

        assert!(!arr.is_empty(1));
        let loaded: u64 = arr.load(1).unwrap();
        assert_eq!(loaded, 0);
    }

    #[test]
    fn inline_store_max_value() {
        let arr: InlineValueArray<u64> = InlineValueArray::new();

        arr.store(2, &u64::MAX);

        let loaded: u64 = arr.load(2).unwrap();
        assert_eq!(loaded, u64::MAX);
    }

    #[test]
    fn inline_update_in_place_returns_noop() {
        let arr: InlineValueArray<u64> = InlineValueArray::new();

        arr.store(5, &100u64);

        let handle: RetireHandle = arr.update_in_place(5, &200u64);

        assert_eq!(handle, RetireHandle::Noop);

        let loaded: u64 = arr.load(5).unwrap();
        assert_eq!(loaded, 200);
    }

    #[test]
    fn inline_take_returns_value_and_empties() {
        let arr: InlineValueArray<u64> = InlineValueArray::new();

        arr.store(7, &77u64);
        assert!(!arr.is_empty(7));

        let taken: u64 = arr.take(7).unwrap();
        assert_eq!(taken, 77);
        assert!(arr.is_empty(7));
        assert!(arr.take(7).is_none());
    }

    #[test]
    fn inline_move_slot_terminal_value() {
        let src: InlineValueArray<u64> = InlineValueArray::new();
        let dst: InlineValueArray<u64> = InlineValueArray::new();

        src.store(3, &999u64);

        src.move_slot(&dst, 3, 8);
        src.clear(3);

        assert!(src.is_empty(3));
        assert!(!dst.is_empty(8));
        assert_eq!(dst.load(8).unwrap(), 999);
    }

    #[test]
    fn inline_move_slot_layer_pointer() {
        let src: InlineValueArray<u64> = InlineValueArray::new();
        let dst: InlineValueArray<u64> = InlineValueArray::new();

        // Use a real allocation for provenance-safe layer pointer simulation.
        let layer_ptr: *mut u8 = Box::into_raw(Box::new(0u8));
        src.store_layer(4, layer_ptr);

        src.move_slot(&dst, 4, 11);
        src.clear(4);

        assert!(src.is_empty(4));
        assert!(dst.is_layer(11));
        assert_eq!(dst.load_layer(11), layer_ptr);

        // Reclaim the allocation.
        unsafe { drop(Box::from_raw(layer_ptr)) };
    }

    #[test]
    fn inline_layer_vs_value_discrimination() {
        let arr: InlineValueArray<u64> = InlineValueArray::new();

        // Store a terminal value at slot 0.
        arr.store(0, &42u64);
        assert!(!arr.is_layer(0));
        assert!(arr.load(0).is_some());

        // Store a layer pointer at slot 1 using a provenance-safe pointer.
        let ptr: *mut u8 = Box::into_raw(Box::new(0u8));
        arr.store_layer(1, ptr);
        assert!(arr.is_layer(1));
        assert_eq!(arr.load_layer(1), ptr);

        // load() on a layer slot returns None (it's not a terminal value).
        assert!(arr.load(1).is_none());

        // Reclaim the allocation.
        unsafe { drop(Box::from_raw(ptr)) };
    }

    #[test]
    fn inline_f64_round_trip() {
        let arr: InlineValueArray<f64> = InlineValueArray::new();

        arr.store(0, &std::f64::consts::PI);
        let loaded: f64 = arr.load(0).unwrap();
        assert!((loaded - std::f64::consts::PI).abs() < f64::EPSILON);
    }

    #[test]
    fn inline_bool_round_trip() {
        let arr: InlineValueArray<bool> = InlineValueArray::new();

        arr.store(0, &true);
        assert!(arr.load(0).unwrap());

        arr.store(1, &false);
        assert!(!arr.load(1).unwrap());
    }

    #[test]
    fn inline_cleanup_is_noop() {
        let arr: InlineValueArray<u64> = InlineValueArray::new();
        arr.store(0, &42u64);

        // cleanup is a no-op for inline values (Copy, no heap).
        unsafe { arr.cleanup(0) };

        // Value should still be readable (cleanup doesn't clear).
        // In practice, cleanup is called from Drop which has &mut access.
    }
}

#[cfg(test)]
mod retire_handle_tests {
    use crate::policy::RetireHandle;

    #[test]
    fn noop_variant() {
        let handle = RetireHandle::Noop;
        assert_eq!(handle, RetireHandle::Noop);
    }

    #[test]
    fn ptr_variant() {
        // Use NonNull::dangling() for a provenance-safe non-null test pointer
        // that is never dereferenced.
        let ptr: *mut u8 = std::ptr::NonNull::<u8>::dangling().as_ptr();
        let handle = RetireHandle::Ptr(ptr);
        match handle {
            RetireHandle::Ptr(p) => assert_eq!(p, ptr),
            RetireHandle::Noop => panic!("expected Ptr"),
        }
    }
}

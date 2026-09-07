
# Notes

- `extend_from_slice(&key)` calls memcpy, which is relatively slow, since most keys are relatively short. For now there's a specialized version toavoid memcpy calls.
    Wild copy 16 bytes in a loop is faster, but would require a guard against overflow from the caller side. (We probably can do that). 
- Comparing two slices of unknown length calls memcmp. Same as above, we can do a specialized version.

fastcmp and fastcpy both employ the same trick, to compare slices of odd length, e.g. 2 operations unconditional on 4 bytes, instead 3 operations with conditionals (1 4byte, 1 2byte, 1 1byte).
[1, 2, 3, 4, 5, 6, 7]
[1, 2, 3, 4]
         [4, 5, 6, 7]

- Since the hashmap writes the values on every key insert/update, the values like expull should be small. Therefore inlining of the values has been removed.
- Currently the first call to Expull will get a capacity of 0. It would be beneficial if it could be initialized with some memory, so that the first call doesn't have to allocate. But that would mean we don't have `Default` impls.

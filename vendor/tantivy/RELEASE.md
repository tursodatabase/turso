# Releasing a new Tantivy Version

## Steps

1. Identify new packages in workspace since last release
2. Identify changed packages in workspace since last release
3. Bump version in `Cargo.toml` and their dependents for all changed packages
4. Update version of root `Cargo.toml`
5. Publish version starting with leaf nodes
6. Set git tag with new version


[`cargo-release`](https://github.com/crate-ci/cargo-release) will help us with steps 1-5:

Replace prev-tag-name
```bash
cargo release --workspace --no-publish -v --prev-tag-name 0.24 --push-remote origin minor --no-tag
```

`no-tag` or it will create tags for all the subpackages

cargo release will _not_ ignore unchanged packages, but it will print warnings for them.
e.g. "warning: updating ownedbytes to 0.10.0 despite no changes made since tag 0.24"

We need to manually ignore these unchanged packages
```bash
cargo release --workspace --no-publish -v --prev-tag-name 0.24 --push-remote origin minor --no-tag --exclude tokenizer-api
```

Add `--execute` to actually publish the packages, otherwise it will only print the commands that would be run.

### Tag Version
```bash
git tag 0.25.0
git push upstream tag 0.25.0
```



fn main() {
    let mut build = cc::Build::new();
    build.file("src/kvstore.c");
    build.include("include");
    if !cfg!(windows) {
        build.flag("-Wno-unused-parameter");
    }
    build.compile("kvstore");

    println!("cargo:rerun-if-changed=src/kvstore.c");
}

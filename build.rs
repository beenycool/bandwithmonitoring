fn main() {
    #[cfg(windows)]
    {
        let manifest = std::path::Path::new("assets").join("bandwith.exe.manifest");
        println!("cargo:rerun-if-changed={}", manifest.display());
        embed_resource::compile(&manifest, embed_resource::NONE)
            .manifest_optional()
            .unwrap();
    }
}

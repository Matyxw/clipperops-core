fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/clipper.proto");
    tonic_build::compile_protos("proto/clipper.proto")?;
    Ok(())
}
mod cli;
mod mcp;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::run().await
}

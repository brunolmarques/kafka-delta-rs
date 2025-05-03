# Latest official Rust image with VS Code goodies pre-installed
# stable toolchain, rust-analyzer, clippy, etc.
FROM mcr.microsoft.com/vscode/devcontainers/rust:latest

# Add any system libs or tools you need for the project
RUN apt-get update && apt-get install -y libssl-dev pkg-config

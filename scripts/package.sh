#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
target=armv7-unknown-linux-gnueabi
output="${CARGO_TARGET_DIR:-target}/$target/release/pb-inpx-dl"

cargo zigbuild --locked --release --features device --target "$target.2.17"
mkdir -p dist/applications/icons dist/system/config
install -m 755 "$output" dist/applications/pb-inpx-dl.app
install -m 644 assets/launcher/pb-inpx-dl.bmp assets/launcher/pb-inpx-dl_f.bmp dist/applications/icons/
install -m 644 pb-inpx-dl.toml dist/system/config/pb-inpx-dl.toml
install -m 644 README.md LICENSE dist/
install -m 644 ui/icons/ATTRIBUTION.txt dist/Font-Awesome-ATTRIBUTION.txt
(
    cd dist
    rm -f pb-inpx-dl-pb634.zip
    zip -q pb-inpx-dl-pb634.zip applications/pb-inpx-dl.app \
        applications/icons/pb-inpx-dl.bmp applications/icons/pb-inpx-dl_f.bmp \
        system/config/pb-inpx-dl.toml README.md LICENSE Font-Awesome-ATTRIBUTION.txt
)

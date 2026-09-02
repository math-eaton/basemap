#!/bin/bash
# Upload sprites and fonts to R2

#TODO

BUCKET_NAME="grid3-maptiles"
R2_ENDPOINT="<your-cloudflare-account-id>.r2.cloudflarestorage.com"

# Build sprites first
cd ../sprites
cargo build --release
make
cd ../app

# Upload sprites to R2
wrangler r2 object put ${BUCKET_NAME}/sprites/v4/light.json --file ../sprites/dist/light.json
wrangler r2 object put ${BUCKET_NAME}/sprites/v4/light.png --file ../sprites/dist/light.png
wrangler r2 object put ${BUCKET_NAME}/sprites/v4/light@2x.json --file ../sprites/dist/light@2x.json
wrangler r2 object put ${BUCKET_NAME}/sprites/v4/light@2x.png --file ../sprites/dist/light@2x.png

# ditto fonts
# wrangler r2 object put ${BUCKET_NAME}/fonts/... --file ...
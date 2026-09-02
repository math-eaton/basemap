## To generate a maplibre-spec spritesheet for webmap icons, use the *spreet* CLI tool.

https://github.com/flother/spreet

---

### To install binaries locally with *cargo*:

`cargo install spreet`

then add to PATH

`echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> ~/.bashrc`
`source ~/.bashrc`

---

Save icons e.g., SVG, in a single folder, then input it to spreet and define an output name:

`spreet --sdf --retina --unique --minify-index-file  maki/ output/maki@2x`

`spreet --sdf --unique --minify-index-file  maki/ output/maki`

remove `--minify-index-file` to pretty-print the index json


to loop through all input svg dirs in cwd:

```bash
for dir in */; do
	[ "$dir" = "output/" ] && continue
	name="${dir%/}"
	spreet --sdf --retina --unique --minify-index-file "$dir" "output/${name}@2x"
	spreet --sdf --unique --minify-index-file "$dir" "output/${name}"
done
```
---


### Or: use the spreet docker image

```bash
docker run \
    -v $(pwd)/maki:/app/icons \
    -v $(pwd)/output:/app/output \
    ghcr.io/flother/spreet \
    --sdf --retina --unique --minify-index-file \
    icons \
    output/maki@2x

docker run \
    -v $(pwd)/maki:/app/icons \
    -v $(pwd)/output:/app/output \
    ghcr.io/flother/spreet \
    --sdf --unique --minify-index-file \
    icons \
    output/maki
```

and to loop through cwd:

```bash
for dir in */; do
    [ "$dir" = "output/" ] && continue
    name="${dir%/}"
    docker run \
        -v "$(pwd)/${name}:/app/icons" \
        -v "$(pwd)/output:/app/output" \
        ghcr.io/flother/spreet \
        --sdf --retina --unique --minify-index-file \
        icons "output/${name}@2x"
    docker run \
        -v "$(pwd)/${name}:/app/icons" \
        -v "$(pwd)/output:/app/output" \
        ghcr.io/flother/spreet \
        --sdf --unique --minify-index-file \
        icons "output/${name}"
done
```
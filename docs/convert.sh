#!/bin/bash

# You may need something like this
# export PATH="$HOME/.rubies/ruby-3.4.6/bin:$PATH"
#

# Root output directory
VERSION=${1:-0.0}
OUT_DIR=${2:-build}

# Clean build directory
if [ -e "$OUT_DIR/.build_output_dir" ]; then
    rm -rf "$OUT_DIR"
fi
mkdir -p "$OUT_DIR"
touch "$OUT_DIR/.build_output_dir"


SRC_DIR="./src"

# Resolve full paths
SRC_DIR_ABS="$(realpath "$SRC_DIR")"
OUT_DIR_ABS="$(realpath "$OUT_DIR")"

# Copy image files
find ./src -type d -name "images" | while read -r IMG_DIR; do
    REL_IMG_DIR="${IMG_DIR#./src/}"
    DEST_IMG_DIR="$OUT_DIR/$REL_IMG_DIR"
    echo "Copying images: $REL_IMG_DIR -> $DEST_IMG_DIR"
    mkdir -p "$DEST_IMG_DIR"
    cp -R "$IMG_DIR/" "$DEST_IMG_DIR/"
done

# Enter output directory
pushd "$OUT_DIR" > /dev/null

# Convert .adoc files and preserve structure
find "$SRC_DIR_ABS" -type f -name "*.adoc" | while read -r SRC_FILE; do
    REL_PATH="${SRC_FILE#$SRC_DIR_ABS/}"
    OUT_FILE="${REL_PATH%.adoc}.html"
    OUT_PATH="$OUT_DIR_ABS/$OUT_FILE"
    mkdir -p "$OUT_DIR_ABS"
    echo "Converting: $SRC_FILE -> $OUT_PATH"
    asciidoctor \
        -a source-highlighter=rouge \
        -a icons=font \
        -a iconsdir= \
        -a linkcss \
        -a project-version=${VERSION} \
        -a pyvgx-version=${VERSION} \
        -o "$OUT_PATH" "$SRC_FILE"
done

# Return to original dir
popd > /dev/null

echo "✅ Conversion complete. Output is in '$OUT_DIR_ABS'"

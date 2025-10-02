#!/bin/bash

# Root output directory
OUT_DIR="build"

# Clean build directory
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

# Copy image files
find ./src -type d -name "images" | while read -r IMG_DIR; do
    REL_IMG_DIR="${IMG_DIR#./}"
    DEST_IMG_DIR="$OUT_DIR/$REL_IMG_DIR"
    echo "Copying images: $REL_IMG_DIR -> $DEST_IMG_DIR"
    mkdir -p "$DEST_IMG_DIR"
    cp -R "$IMG_DIR/" "$DEST_IMG_DIR/"
done

# Convert .adoc files and preserve structure
find ./src -type f -name "*.adoc" | while read -r SRC_FILE; do
    REL_PATH="${SRC_FILE#./}"
    OUT_FILE="${REL_PATH%.adoc}.html"
    OUT_PATH="$OUT_DIR/$OUT_FILE"
    mkdir -p "$(dirname "$OUT_PATH")"
    echo "Converting: $SRC_FILE -> $OUT_PATH"
    asciidoctor \
        -a icons=font \
        -a iconsdir= \
        -a linkcss \
        -o "$OUT_PATH" "$SRC_FILE"
done

echo "✅ Conversion complete. Output is in '$OUT_DIR'"



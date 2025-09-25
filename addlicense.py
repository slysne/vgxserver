import os
import sys
import re

PROJECT_NAME = "VGX Server"
PROJECT_DESC = "Low-latency distributed engine for plugin-based graph and vector search"
AUTHOR = "Stian Lysne"
COPYRIGHT_YEAR = "2025"
COMPANY = "Rakuten, Inc."
EMAIL = "..."


LICENSE = f"""
=== {PROJECT_NAME} ===
{PROJECT_DESC}

Module:  {{module:}}
File:    {{fname:}}
Author:  {AUTHOR} <{EMAIL}>

Copyright © {COPYRIGHT_YEAR} {COMPANY}

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

LICENSE_LINES = LICENSE.split('\n')

LICENSE_SOURCE = {
    'python':   ["#"*79] + [ "# {}".format(line) for line in LICENSE_LINES ] + ["#"*79],
    'c':        ["/" + "*"*78] + [ " * {}".format(line) for line in LICENSE_LINES ] + [" " + "*"*77  + "/"],
    'html':     ["<!--"] + ["  {}".format(line) for line in LICENSE_LINES ] + ["-->"]
    # Add other file types...
}

print(LICENSE)


EXTENSIONS = {
    '.py': 'python',
    '.c': 'c',
    '.h': 'c',
    '.js': 'c',
    '.css': 'c',
    '.sh': 'python',  # Shell uses #
    '.html': 'html',
    # add more
}


def get_module_name(filepath):
    if "cxlib" in filepath:
        return "cxlib"
    if "framehash" in filepath:
        return "framehash"
    if "comlib" in filepath:
        return "comlib"
    if "pyvgx" in filepath:
        return "pyvgx"
    if "vgx" in filepath:
        return "vgx"
    return "general"



def has_license_header(lines):
    return any("Licensed under the Apache License" in line for line in lines[:10])



def is_minified(data):
    lines = data.count('\n')
    if lines < 1:
        return True
    white = data.count(' ') + lines
    size = len(data)
    
    white_ratio = white / size
    avg_line_size = sum([len(line) for line in data.split('\n')]) / lines

    if white_ratio < 0.05 and avg_line_size > 300:
        return True

    return False
    


def is_binary_file(filepath, blocksize=1024):
    """
    Detects whether a file is binary by checking for non-text bytes.
    Reads the first block of the file and checks for non-printable characters.
    """
    try:
        with open(filepath, 'rb') as f:
            chunk = f.read(blocksize)
            if not chunk:
                return False  # empty files are considered text
    except Exception:
        return True  # can't read? safer to treat as binary

    # Heuristic: if more than 30% of bytes are non-text, assume binary
    text_characters = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x7F)))
    nontext = [b for b in chunk if b not in text_characters]

    if len(nontext) / len(chunk) > 0.3:
        return True

    return False




def read_file_with_bom(filepath):
    """
    Reads the file and returns (has_bom, is_min, lines)
    If binary files, lines will be empty
    """
    if is_binary_file(filepath):
        return False, False, []

    with open(filepath, 'rb') as f:
        raw = f.read()

    has_bom = raw.startswith(b'\xef\xbb\xbf')
    text = raw.decode('utf-8-sig' if has_bom else 'utf-8')

    is_min = is_minified(text)

    lines = text.splitlines(keepends=True)
    return has_bom, is_min, lines



def write_file_preserving_bom(filepath, text, has_bom):
    """
    Writes the file with or without a BOM based on the original state
    """
    with open(filepath, 'wb') as f:
        if has_bom:
            f.write(b'\xef\xbb\xbf')
        f.write(text.encode('utf-8'))



def strip_empty_top_lines(lines):
    # Remove leading blank lines
    while lines and lines[0].strip() == '':
        lines.pop(0)


def strip_empty_bottom_lines(lines):
    # Remove trailing blank lines and add final newline
    while lines and lines[-1].strip() == '':
        lines.pop()
    if lines and not lines[-1].endswith('\n'):
        lines[-1] = lines[-1] + '\n'
    





def strip_existing_comment_block(lines, style):
    """
    Remove top-of-file comments or docstring blocks.
    Returns the cleaned list of lines.
    """
    strip_empty_top_lines(lines)

    if style == 'python':
        # Remove lines starting with #
        i = 0
        while i < len(lines) and lines[i].strip().startswith('#'):
            i += 1
        return lines[i:]  # return remaining lines

    elif style == 'c':
        code = ''.join(lines)
        match = re.match(r'^\s*/\*.*?\*/\s*', code, re.DOTALL)
        if match:
            end = match.end()
            remaining = code[end:]
            return remaining.splitlines(True)
        else:
            return lines

    elif style == 'html':
        first = []
        if lines[0].strip() == '<!DOCTYPE html>':
            first = lines[0:1]
            code = ''.join(lines[1:])
        else:
            code = ''.join(lines)
        match = re.match(r'^\s*<!--.*?-->\s*', code, re.DOTALL)
        if match:
            end = match.end()
            remaining = code[end:]
            return first + remaining.splitlines(True)  # Keep line endings
        else:
            return lines
                    

    else:
        return lines



LINE_COUNT_IN = 0
LINE_COUNT_OUT = 0
FILE_COUNT = 0


def process_file(filepath, add=True):
    global LINE_COUNT_IN
    global LINE_COUNT_OUT
    global FILE_COUNT
    ext = os.path.splitext(filepath)[1]

    style = EXTENSIONS.get(ext)
    if not style:
        print( "Skipping {}".format(filepath) )
        return

    FILE_COUNT += 1

    has_bom, is_min, lines = read_file_with_bom(filepath)

    if not lines or is_min:
        print( "Skipping {}".format(filepath) )
        return

    LINE_COUNT_IN += len(lines)
    

    lines = strip_existing_comment_block(lines, style)
    strip_empty_top_lines(lines)
    strip_empty_bottom_lines(lines)

    if add is True and style:
        name = os.path.basename(filepath)
        modname = get_module_name(filepath)
        header_lines = LICENSE_SOURCE[style]
        header = "\n".join(header_lines)
        header = header.format(fname=name, module=modname)
        header += "\n\n"
    else:
        header = ""


    LINE_COUNT_OUT += len(lines) + len(header.split('\n'))

    if style == 'html' and lines[0].strip() == '<!DOCTYPE html>':
        text = lines[0] + header + ''.join(lines[1:])
    else:
        text = header + ''.join(lines)
    
    write_file_preserving_bom(filepath, text, has_bom)

    print( "Processed {} ({}) - Total processed files:{} Lines in:{} out:{}".format(filepath, "add" if add else "remove", FILE_COUNT, LINE_COUNT_IN, LINE_COUNT_OUT) )



def main():
    what = "add"
    if len(sys.argv) < 2:
        print( "need repo root as arg" )
        sys.exit(1)
    if len(sys.argv) > 1:
        source_root_dir = sys.argv[1]
        if len(sys.argv) > 2:
            what = sys.argv[2]
    # Walk directories
    for root, _, files in os.walk( source_root_dir ):
        for name in files:
            filepath = os.path.join(root, name)
            if what == "add":
                process_file(filepath, add=True)
            elif what == "remove":
                process_file(filepath, add=False)



if __name__ == "__main__":
    main()



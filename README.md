# <img src="./vgx/src/resources/WEB-ROOT/artifacts/logo_w-b.png" alt="VGX" width="200"/>

Distributed engine for plugin-based graph and vector search

## Why VGX?

**VGX + 1 is WHY**  
**+1 is you**

Originally short for **Vector Graph indeX**, VGX is a high-performance, distributed engine for building custom search and recommendation services using Python plugins. It combines real-time graph traversal, vector similarity, and expressive filtering into a unified platform, backed by a native C-core for speed and scalability. Developers can implement service logic using the **PyVGX** C-extensions, expose it as HTTP endpoints, and automatically scale across a sharded, replicated back-end. With built-in support for ANN search, dynamic graphs, expression-based filtering, and pluggable infrastructure, VGX makes it easy to develop powerful, low-latency systems for semantic search, recommendation, autocomplete, and more.

## About This Project

VGX was originally developed in-house at Rakuten, Inc. between 2014 and 2025 as a versatile platform for live services. Built from the ground up, it focuses on maximizing memory efficiency and hardware utilization while delivering consistent low-latency performance. In 2025, we open-sourced the platform to share its capabilities with the wider community and foster collaboration.

## Getting Started

You will need Python 3.9 or higher and one of the supported operating systems:

- **macOS**: 14 (Sonoma) or higher
- **Linux**: glibc 2.34 or higher (e.g. Ubuntu 22.04+)
- **Windows**: 10 or higher

It is usually a good idea to use a virtual environment to keep things isolated:

**MacOS / Linux venv setup**
```bash
python3 -m venv vgxenv
source vgxenv/bin/activate
```

**Windows venv setup**
```bat
python -m venv vgxenv
call vgxenv\Scripts\activate.bat
```

**Install PyVGX**
```bash
pip install pyvgx
```

## Hello VGX

Now let's define and expose a service using VGX:

**Plugin Code**
```python
# hello.py
from pyvgx import *

system.Initialize("hello")

# This function will be exposed as an HTTP endpoint
def Hello(request: PluginRequest, message: str = "nothing"):
    response = PluginResponse()
    response.Append(f"Hi, you said {message}")
    return response

system.AddPlugin(Hello)

system.StartHTTP(9000) # main port=9000, admin port=9001
print("Visit 'http://127.0.0.1:9001' for admin" )

# Until SIGINT
system.RunServer()
```

**Start Service**
```bash
# Run the service
python hello.py
```

**Send Request**
```bash
# Send a request
curl http://127.0.0.1:9000/vgx/plugin/Hello?message=hello!
```

## Simple Graph Examples

### Example 1: Build relationships and ask a question

```python
from pyvgx import *

# Make some friends
g = Graph( "friends" )
g.Connect( "Alice", "knows", "Bob" )
g.Connect( "Alice", "knows", "Charlie" )
g.Connect( "Alice", "knows", "Diane" )
g.Connect( "Charlie", "likes", "coffee" )

# Which of Alice's friends likes coffee?
g.Neighborhood(
    "Alice",
    arc      = "knows",
    neighbor = {
        'arc'      : "likes",
        'neighbor' : "coffee"
    }
) # -> ['Charlie']
```

### Example 2: Build a vector graph and find most similar match

```python
from pyvgx import *
import random

# Connect root to many vertices with vectors
root = g.NewVertex( "root" )
for n in range( 10000 ):
    v = g.NewVertex( f"v{n}" )
    v.SetVector( g.sim.rvec(1024) ) # assign a random vector
    r = g.Connect( root, "to", v )

# Select a target and derive a probe (add noise) from its vector
target = g["v7357"]
probe = [x + 0.5 * (random.random()-0.5) for x in target.GetVector().external]

# Run a query around root and sort by similarity to probe vector
g.Neighborhood(
    id="root",
    hits=3,
    fields=F_ID|F_RANK,
    vector=probe,
    sortby=S_RANK,
    rank="cosine(vector, next.vector)"
) # -> ['{"id": "v7357", "rankscore": 0.97...}', ...]
```

## VGX Demo System

If you want to see a larger demo system in action, type the following in a terminal:

```bash
# Start a multi-node VGX system
vgxdemosystem multi
```

This will start many server instances (using ~16GB RAM) and open a system dashboard in your web browser:

# <img src="https://github.com/slysne/vgxserver/blob/main/docs/src/pyvgx/images/ui_system.png" alt="SystemDashboard" width="768"/>

Allow startup to finish and then try to send a query to the dispatcher running on port 9990:

```bash
# Run test queries, return JSON search result
curl -s http://127.0.0.1:9990/vgx/plugin/search?name=7357 | jq
curl -s http://127.0.0.1:9990/vgx/plugin/search?name=index | jq
```

You can see how the demo is implemented here: [vgxdemoservice.py](https://github.com/slysne/vgxserver/blob/main/pyvgx/src/py/vgxdemoservice.py) and [vgxdemoplugin.py](https://github.com/slysne/vgxserver/blob/main/pyvgx/src/py/vgxdemoplugin.py) 


To stop the system type this in a terminal:

```bash
vgxdemoservice stop
```

## Documentation

Comprehensive API documentation is available. 

- [VGX Documentation Home](https://slysne.github.io/vgxserver)

A few quick links:

- [PyVGX Tutorial](https://slysne.github.io/vgxserver/pyvgx/tutorial.html)
- [PyVGX Reference](https://slysne.github.io/vgxserver/pyvgx/reference.html)
- [PyVGX Compact Reference](https://slysne.github.io/vgxserver/pyvgx/shortref.html)
- [VGX Expression Language](https://slysne.github.io/vgxserver/pyvgx/evaluator/evaluator.html)
- [VGX Server](https://slysne.github.io/vgxserver/pyvgx/service/service.html)

Recommendation: Read the [Tutorial](https://slysne.github.io/vgxserver/pyvgx/tutorial.html) first. It covers some of the graph basics without going too deep.

## Building from Source

If you want to build PyVGX from source or contribute to development:

### Prerequisites

- **Python 3.9-3.13**: Required for building and testing
- **Docker**: Required for manylinux builds (Linux wheels)
- **C Compiler**: clang (automatically installed in Docker for manylinux builds)

### Quick Build

```bash
# Clone the repository
git clone https://github.com/slysne/vgxserver.git
cd vgxserver

# Set version (optional - reads from VERSION file if not specified)
echo "3.6.0" > VERSION

# Build a local wheel for your platform
make build-local

# Or specify version explicitly
make build-local VERSION=3.6.0
```

### Building Manylinux Wheels (Linux)

The project supports building portable Linux wheels (x86_64 and ARM64) using pypa/manylinux Docker images:

```bash
# Build for x86_64 (all Python versions 3.9-3.13)
./build-manylinux.sh 3.6.0

# Build for specific Python versions
./build-manylinux.sh 3.6.0 "cp311-cp311 cp312-cp312"

# Build and test in one command
./build-manylinux.sh 3.6.0 --test

# Build for ARM64/aarch64 (requires ARM host or QEMU)
./build-manylinux-aarch64.sh 3.6.0

# Using Makefile (reads VERSION file automatically)
make build-manylinux
make build-manylinux-arm64

# Build both architectures
make build-all-manylinux
```

### Testing Wheels

```bash
# Test all built wheels (auto-detects Python version from filename)
./test-wheels.sh

# Or via Makefile
make test

# Test wheels from specific directory
./test-wheels.sh dist/
```

The test script runs 6 comprehensive tests per wheel:
1. Import pyvgx module
2. Version consistency check
3. vgxadmin CLI command
4. vgxadmin module import
5. vgxinstance module import  
6. vgxdemoservice functionality

### Advanced Build Options

```bash
# Using cibuildwheel (install first: pip install cibuildwheel)
make cibuildwheel VERSION=3.6.0

# Build specific Python version and architecture
make cibuildwheel VERSION=3.6.0 PYVER=312 ARCH=x86_64

# Clean build artifacts
make clean
```

For detailed build instructions and troubleshooting:
- [Manylinux Build Guide](docs/MANYLINUX_BUILD.md) - Comprehensive build documentation
- [Quick Reference](docs/MANYLINUX_QUICKREF.md) - Common commands and workflows
- [Setup Guide](docs/MANYLINUX_SETUP.md) - Initial setup documentation

### GitHub Actions Build & Release

**Automated builds triggered by:**
- Tags (v*) or GitHub Releases → Release version from tag
- Pull Requests → Test builds with VERSION file + dev timestamp
- Manual trigger → Optional parameters (version, Python versions, architectures)

**To create a release:**
1. `echo "3.7.0" > VERSION && git commit -am "Bump version" && git push`
2. Create GitHub release with tag `v3.7.0` or `git tag v3.7.0 && git push origin v3.7.0`
3. Download wheels from Actions artifacts (30-day retention)

### Development Build

```bash
# Install in development mode
pip install -e .

# Run tests
python -m pytest pyvgx/test/ -v
```

## Maintainers

This project was open-sourced by **Rakuten, Inc.** and is currently maintained by:

- **Stian Lysne** – [@slysne](https://github.com/slysne)
- Contact: slysne.dev [at] gmail [dot] com

For questions, issues, or contributions, feel free to open an issue or pull request.


## License

This project is licensed under the Apache License Version 2.0. See [LICENSE](./LICENSE) for details.

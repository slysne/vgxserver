# <img src="./vgx/src/resources/WEB-ROOT/artifacts/logo_w-b.png" alt="VGX" width="200"/>

Distributed engine for plugin-based graph and vector search

## Why VGX

*VGX + 1 is WHY*

You're the +1. Originally short for **Vector Graph Index**, VGX is a high-performance, distributed engine for building custom search and recommendation services using Python plugins. It combines real-time graph traversal, vector similarity, and expressive filtering into a unified platform, backed by a native C-core for speed and scalability. Developers can implement service logic using Python, expose it as HTTP endpoints, and automatically scale across a sharded, replicated back-end. With built-in support for ANN search, dynamic graphs, expression-based filtering, and pluggable infrastructure, VGX makes it easy to develop powerful, low-latency systems for semantic search, recommendation, autocomplete, and more.

## Install

You will need Python 3.12 or higher. It may be a good idea to use a virtual environment to keep things isolated:

### venv

**MaxOS / Linux**
```bash
python3 -m venv vgxenv
source vgxenv/bin/activate
```

**Windows**
```bat
python3 -m venv vgxenv
call vgxenv\Scripts\activate.bat
```



## Hello VGX

Here’s how simple it is to define and expose a custom service using VGX:

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

## VGX Demo System

If you want to see a more interesting demo system in action, type the following in a terminal:

```bash
# Start a multi-node VGX system
vgxdemosystem multi
```

This will start many server instances (using ~16GB RAM) and open the admin interface in your web browser.

# <img src="./docs/src/pyvgx/images/ui_system.png" alt="SystemDashboard" width="768"/>

Allow startup to finish and then try to send a query to the dispatcher running on port 9990:

```bash
# Run a test query, returns JSON search result
curl -s http://127.0.0.1:9990/vgx/plugin/search?name=7357 | jq
```






To stop the system type this in a terminal:

```bash
vgxdemoservice stop
```









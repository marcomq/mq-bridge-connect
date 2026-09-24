# mq-bridge-connect (Node.js)

An unofficial Redpanda Connect compatibility plugin for
[mq-bridge](https://www.npmjs.com/package/mq-bridge). It lets mq-bridge routes
use Redpanda Connect (Benthos) connectors as endpoints. The package contains no
JavaScript implementation: it loads the compiled Rust plugin, which loads its Go
sibling library from the same directory.

> **Unofficial.** Not affiliated with, endorsed by or supported by Redpanda
> Data, Inc. or the Benthos project. Report issues at
> <https://github.com/marcomq/mq-bridge-connect/issues>.

```console
npm install mq-bridge mq-bridge-connect
```

The libraries (about 70 MB compressed per platform) are too large for the npm
package, so they are not in it. `register()` uses the first of:

1. `MQ_BRIDGE_CONNECT_LIBRARY`, the absolute path of the plugin library;
2. the download cache (`~/.cache`, `~/Library/Caches` or `%LOCALAPPDATA%`,
   under `mq-bridge-connect/`; override with `MQ_BRIDGE_CONNECT_CACHE`);
3. a conda environment (`$CONDA_PREFIX`) or Homebrew install:
   `conda install -c marcomq mq-bridge-connect` or
   `brew install marcomq/tap/mq-bridge-connect`;
4. otherwise it downloads this version's
   [GitHub release archive](https://github.com/marcomq/mq-bridge-connect/releases)
   into the cache, checked against the sha256 pinned in the package.

Run `npx mq-bridge-connect` to download ahead of time, e.g. in a Docker build.
Set `MQ_BRIDGE_CONNECT_NO_DOWNLOAD=1` to forbid the download, or
`MQ_BRIDGE_CONNECT_DOWNLOAD_URL` to fetch the archives from a mirror. A conda or
Homebrew install is used whatever its version, so keep it in step with this
package.

```javascript
import { Route } from "mq-bridge";
import { register } from "mq-bridge-connect";

register(); // call once, before starting routes

const route = Route.fromStr(`
generate_to_file:
  input:
    custom:
      name: connect
      config:
        connector: generate
        count: 10
        interval: ""
        mapping: "root.id = counter()"
  output:
    file:
      path: "out.jsonl"
`);
route.start();
route.join(); // block until the route stops
```

`register()` returns the endpoint name (`connect`) and is a no-op when called
again. See the
[repository README](https://github.com/marcomq/mq-bridge-connect#configuring-an-endpoint)
for the endpoint configuration.

A plugin is native code with the same privileges as the Node process — install
it as you would any other native dependency.

## Licences

The two libraries statically link several hundred third-party modules.
`THIRD_PARTY_NOTICES`, shipped next to the libraries in the release archive,
carries the full text of every licence involved. Keep it with the libraries if
you repackage them.

## Building the package

The release workflow writes `checksums.json` from the archives it built, copies
the licence files in, and runs `npm pack`. For local development, put both
libraries in `prebuilds/<platform>/` (e.g. `darwin-arm64`, `linux-x64-gnu`);
they take precedence over the cache and system installs.

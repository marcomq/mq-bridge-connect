# Installing the mq-bridge Redpanda Connect plugin

This directory is one unit. It contains:

| File | What it is |
| :--- | :--- |
| `libmq_bridge_redpanda.{so,dylib}` | the plugin mq-bridge loads |
| `libmq_bridge_redpanda_go.{so,dylib}` | its Go sibling, loaded by the plugin |
| `THIRD_PARTY_NOTICES` | every third-party licence linked into the two libraries, in full |
| `LICENSE-MIT`, `LICENSE-APACHE` | this project's own dual licence |

## Keep the notices with the libraries

`THIRD_PARTY_NOTICES` is not documentation. The two libraries statically link
several hundred third-party modules whose licences require their text and
copyright notices to accompany the binaries, and it is the file that discharges
that. Copying the libraries somewhere without it turns a compliant build into a
distribution that is missing required notices.

So: copy the whole directory, or copy the notices alongside whatever you copy.
If you repackage these libraries into an image, a formula or a package, install
the notices into the image too — not only into a source tarball.

## Where to put it

mq-bridge resolves a plugin by the endpoint name a route asks for, looking for
`libmq_bridge_redpanda.{so,dylib}` (`mq_bridge_redpanda.dll` on Windows) under
`lib/mq-bridge` and plain `lib` on a search path covering:

- `MQB_PLUGIN_DIR`
- the running mq-bridge binary's prefix
- `$CONDA_PREFIX`
- `$HOMEBREW_PREFIX`
- `~/.local/share/mq-bridge/plugins`

The simplest install is to point `MQB_PLUGIN_DIR` at this directory as it is:

```console
export MQB_PLUGIN_DIR=/path/to/this/directory
```

For a prefix-style install, put both libraries in `<prefix>/lib/mq-bridge/` and
the notices beside them:

```console
install -d "$PREFIX/lib/mq-bridge" "$PREFIX/share/doc/mq-bridge-redpanda"
install -m 0755 libmq_bridge_redpanda*.so "$PREFIX/lib/mq-bridge/"
install -m 0644 THIRD_PARTY_NOTICES LICENSE-MIT LICENSE-APACHE \
    "$PREFIX/share/doc/mq-bridge-redpanda/"
```

Both libraries must land in the same directory: the plugin loads its Go sibling
from beside itself.

See `docs/PLUGINS.md` in the mq-bridge repository for the full search order.

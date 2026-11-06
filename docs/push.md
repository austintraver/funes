# Publishing and sharing

Your local memory is a [Lance](https://lancedb.github.io/lance/) dataset, and it shares the way a
dataset does: publish it to a Hugging Face **dataset repo you own** and any machine, teammate — or
anyone, if you make it public — recalls from it with one flag. The Hub is a tier you opt into; you
never need it to use funes locally.

```bash
funes push <user|org>/funes-memory                   # publish your local memory's new chunks there
funes recall "..." --memory <user|org>/funes-memory   # read it back from anywhere (see recall.md)
```

## `funes push`

`funes push <memory>` uploads the embedded chunks your local memory has that the remote doesn't.
Chunks awaiting embedding stay local; after indexing fills their vectors, the next push includes
them. The memory is an `<org>/<repo>` shorthand or a full `hf://…` URI.

On its **first publish**, push also writes the repo's dataset card — what a funes memory is, how to
recall from it, live stats — tagged [`funes`](https://huggingface.co/datasets?other=funes) so every
shared memory is recognizable and discoverable on the Hub. Later pushes keep the stats fresh. A card
you wrote yourself is never touched.

| Flag | Meaning |
| --- | --- |
| `-y`, `--yes` | Skip the wrong-memory confirmation (below). |
| `--compact` | Compact the remote after pushing, even if the unindexed backlog is below the threshold that triggers it. With nothing new to push, compact only. `--force-reindex` is the old name. |

Once 500 pushed rows sit unindexed on the remote, a push also compacts it: it merges the small
fragments, adds those rows to the search indexes, and deletes the old versions.
`funes compact <memory>` does the same without pushing ([index.md](index.md#compaction)).

**The wrong-memory guard.** A push to a memory that holds none of the chunks it would publish asks
before uploading, and says which case it found: sessions named with `--sessions` that the memory
lacks, an empty memory (a first publish), chunks from other hosts only (a new host of yours), or
nothing in common with a memory that held chunks from this host before (what the wrong memory looks like).
Off a terminal it refuses rather than guess; `--yes` overrides. ([`funes add`](add.md) clears this
for you by doing the first push interactively — once that machine has an index of its own to push.)

If your token can't write the target, push says so — recall can still read a memory you can't publish
to.

A memory holds one embedding model's vectors, so a push into a memory embedded with another model than
the local one is refused. A first publish with no chunks to upload still creates the memory with the
local one's model, so every host that binds it next embeds with the same.

One publish per memory at a time on a machine: a push that starts while another is still running
says so and stops, and the next one picks up what it left. The hooks [`funes add`](add.md) installs
run this at session boundaries automatically; see [automation.md](automation.md).

## Keeping secrets out: the push gate, `funes swab`, and `funes scrub`

When TruffleHog is available, indexing redacts detected credentials before storing a session. That
first pass is best-effort: local indexing still works without the scanner because the local memory
has not crossed a publication boundary. It prints a warning that index-time redaction is disabled.

Push is the hard boundary. A separate, **always-on, fail-closed gate** requires TruffleHog and scans
the rows about to leave the machine. It reconstructs content blocks from those rows before scanning,
so a secret split across chunks can be detected. If a block contains a secret, every outgoing row
from that block is held back; unrelated clean rows still publish with a warning. Each held block is
queued by its session id, turn id, and block number. This coordinate identifies the block even when
its source turn has an old timestamp but was indexed only recently. Only when the gate leaves
*nothing* to publish does push exit non-zero (code `2`):

```console
$ funes push <user|org>/funes-memory
scanning 512 chunk(s) for secrets…
hf://datasets/<user|org>/funes-memory: nothing published — held back 3 row(s) with secrets (AWS×2, PrivateKey×1); run `funes swab <user|org>/funes-memory`, then push again
$ echo $?
2
```

A hold-back is easy to miss when the hooks push in the background, so `funes status` scans this
host's pending rows for the bound memory the same way and reports what a push would hold back. After
`funes swab` or `funes scrub` repairs the affected blocks, the next push can publish the repaired rows.

`funes push`, `funes swab`, and `funes scrub` require TruffleHog when they have rows to scan. See the [TruffleHog installation instructions](https://github.com/trufflesecurity/trufflehog#installation). Funes checks [`FUNES_TRUFFLEHOG`](configuration.md#environment-reference) first, then `PATH`, then common install directories.

### What `funes swab` changes

Run `funes swab <memory>` once a day for the memory that push targets. Push can continue running at
session boundaries throughout the day: it keeps withholding secret-bearing rows and adds their block
coordinates to a per-remote local worklist. The worklist stores no block text or scanner findings.
Repeated pushes merge new keys into it, including pushes limited by `--sessions`. A swab and a push
to the same remote take the same lock, so a swab drains a stable set of keys. It clears the worklist
when the last batch succeeds. If interrupted, the next swab reads the worklist again; blocks already
repaired need no further write.

To seed the worklist with rows held before this worklist existed, run `funes push <memory>` once. The
push gate scans the rows still pending for that remote and records any blocks it holds back.

`funes swab <memory>` locates queued blocks by their transcript coordinates. This lookup can read identifier columns across historical fragments. Only the queued blocks' text goes to the secret scanner; unrelated text and vectors are not rewritten. A block from an old conversation indexed today is eligible as soon as a push withholds it.

Where a secret can be located safely, swab replaces it with a `[REDACTED:<detector>]` marker, re-chunks the block, and embeds its replacements. If the secret cannot be located safely or the repaired text still triggers the scanner, swab drops the block.

Each batch with dirty blocks makes one targeted Lance commit. Swab removes obsolete split ids
from the current memory and records them so a later transcript re-index cannot restore their original
text. The next ordinary `funes push <memory>` can publish the repaired rows that the gate previously
held back.

Swab changes what is publishable in the **current local memory**; it does not retract data already
published to the Hub. It also does not immediately remove prior local Lance versions from disk. The
cleanup of old versions reclaims files that are no longer referenced, but a live fragment can still
contain deleted bytes behind a deletion mask. Local compaction does not materialize those deletions.

### What `funes scrub` changes

`funes scrub` repairs the **local derived memory** in place, including sessions whose source
transcripts no longer exist. It takes the local writer lock, reconstructs and scans every stored
block, then makes one replacement commit if it finds a secret:

- A secret whose value can be located safely is replaced with a `[REDACTED:<detector>]` marker. The
  block is re-chunked and its replacement chunks are re-embedded.
- If a finding cannot be reconstructed safely—for example, an encoded value with no reliable byte
  match—the entire block is dropped instead of risking a partial redaction.
- A redaction is kept only if the redacted block scans clean. Excising one match can expose another
  (removing one from a long base64 run re-aligns the window the next is found in), and such a block
  is dropped rather than stored.
- Clean rows retain their existing embeddings. The vector and full-text indexes are rebuilt after
  the replacement.

Scrub removes older dataset versions and files that are no longer referenced. If the current dataset is already clean, scrub only removes old versions; it does not rewrite live fragments. A live fragment can still contain bytes from a row that swab marked deleted.

The source transcripts are never modified. Scrub reports how many secrets and blocks it redacted and
how many rows it had to drop. The next `funes push <memory>` scans the repaired rows again before publishing them.

Scrub does **not** alter an already-published remote. If a live credential reached the Hub, revoke or
rotate it first, then remediate the dataset separately; funes can prevent another upload but does not
automate remote deletion.

## Publishing a selection: `--sessions`

A push ships every embedded local chunk the remote doesn't have. To publish a *selection* instead,
name the sessions:

```bash
funes push <memory> --sessions <session> --sessions <session>
```

Those sessions' embedded chunks are exactly what ships. The list **is** the decision — funes keeps
no record of what you meant to publish, so a selection is made where it takes effect, and an
unrecognized session id fails the push rather than quietly publishing the rest. `funes sessions`
lists the ids, and naming a session publishes every embedded chunk it holds.

The remote is append-only, so a selection is a pre-publication gate and not a remote undo. Nothing
retracts a session once it is up.

This is the surface an agent-driven curation writes into: an agent selects the qualifying sessions
against your criteria and reports them, and the person publishing passes that list.

## Inspecting a memory: `status`

`funes status` shows your local memory, then the memory this host is bound to (by
[`funes bind`](add.md#binding-a-memory)), and an update check. A host set up by a funes from
before the binding is told what its agents were bound to, and what binds it.

`funes status` tells you whether recall is reading your own memory yet, and whether a newer funes
release is out. Its local-index sections distinguish chunks awaiting embedding from source
sessions not yet indexed. Run `funes index` to continue either. Each memory also lists the
languages its sessions are written in, with each one's share, and how many chunks the counts cover
when they miss some.

For the bound memory, one `local push` line says either that this host is up to date or how many
local sessions are pending. This comes from a per-remote receipt kept on this host, so sessions
contributed by other hosts do not distort the result and status never scans the remote to compute
it. Run `funes push <memory>` once to initialize the receipt for an existing memory.

## See also

- [recall.md](recall.md) — reading a shared memory with `--memory`.
- [automation.md](automation.md) — the session-boundary publishing the hooks run.
- [hub-caching.md](hub-caching.md) — how recall over a remote caches to local disk.

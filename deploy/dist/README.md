# Wyrd distribution tarball

This tarball installs the Wyrd roles on a bare-metal / VM Linux host with
systemd, per ADR-0010's substrate order (single binary primary, OCI image the
same binary, compose for eval). One `wyrd` binary serves every role as a
subcommand: `d-server`, `custodian`, `s3` (long-running, installed as units)
plus the `put` / `get` / `demo` CLI roles.

Beside it ships a second, optional binary, `bin/wyrd-validate` — the blackbox
validation tool (design proposal 0017). It is deliberately *not* a role and not a
`wyrd` subcommand: it drives a deployment through its S3 front door exactly the
way a client does, depends on no Wyrd crate, and is run by hand (or by a
launcher) against an endpoint you name — `wyrd-validate` with no arguments
prints its usage and exits non-zero. It gets no systemd unit and no `/etc/wyrd`
config, and it does not link `libfdb_c`, so it runs on a host with no
FoundationDB client installed at all. Ignore it and nothing else changes.

## What this build is

- **Flavor:** `fdb,etcd` — FoundationDB metadata (ADR-0042) + etcd coordination.
- **Linkage:** dynamically linked, glibc ≥ 2.36 (built on Debian bookworm; both
  binaries are bit-identical to the ones in the `wyrd:<version>-fdb` OCI image,
  which carries them at `/usr/local/bin/`). An fdb-backed wyrd is *never* a
  single static binary — FoundationDB does not support static `libfdb_c`
  (architecture doc `07-deployment-view.md` §7.6).
- **Runtime requirement:** `libfdb_c` from `foundationdb-clients` at the exact
  pinned version (see `VERSION`) — for `wyrd`; the installer prints the download
  command if it is missing. The version must match the FDB cluster you deploy
  against. The validator needs no FoundationDB client.

## Install

```sh
sudo ./install.sh                # or --prefix /opt/wyrd
```

The installer creates the `wyrd` system user, installs the binaries to
`<prefix>/bin/wyrd` and `<prefix>/bin/wyrd-validate`, the units to
`/etc/systemd/system/`, per-role configs to `/etc/wyrd/<role>.env` (from the
bundled examples; existing configs are never overwritten), and `/var/lib/wyrd`.
It does **not** enable or start anything.

Then, per role on this host:

1. Edit `/etc/wyrd/<role>.env` — the `.example` alongside documents every
   required flag. The env files mirror the M4 first-deployment blueprint's
   production invocations; `--failure-domain` must be honest (it is what makes
   the erasure-coding durability math real), and the s3 credentials are
   required (auth is fail-closed).
2. `systemctl enable --now wyrd-<role>`

> **Run exactly ONE custodian per cluster.** Single-active is not enforced
> against a distributed metadata store until the etcd Coordination backend
> (#365); two custodians would both self-grant leadership.

Secrets note: `/etc/wyrd/s3.env` is installed `0640 root:wyrd`. A systemd
`LoadCredential=` upgrade for the gateway keys is future work.

## Upgrade

Re-run `install.sh` from the new tarball: both binaries, units, and `.example`
files are refreshed; your `/etc/wyrd/<role>.env` files are preserved. Restart
the units to pick up the new binary.

## Uninstall

```sh
sudo ./install.sh --uninstall            # keeps /etc/wyrd and /var/lib/wyrd
sudo ./install.sh --uninstall --purge    # deletes config AND fragment data
```

`--uninstall` removes both binaries and the units. A custom `--prefix` install
is found automatically: the installer records its prefix in
`/etc/wyrd/install-prefix`, and `--uninstall` reads it (an explicit `--prefix`
on the uninstall command overrides the record).

## Verify an install end-to-end

Unit sanity without a cluster: `systemd-analyze verify
/etc/systemd/system/wyrd-*.service`, and `wyrd` with no arguments prints usage
(so does `wyrd-validate`, and it does so before any FoundationDB client is
installed).

A full three-role bring-up needs FDB + etcd + a D-server fleet. For a
single-host rehearsal, start the backends from the repo's
`deploy/small-multi-node-fdb/` compose fixture, point the env files at the
compose-published endpoints, then `systemctl start` the roles and drive an
object through: `wyrd put … --endpoints …` / `wyrd get …` (or an S3 client
against `wyrd-s3`). The repo's day-one runbook (m4-first-deployment-blueprint)
covers the kill-a-D-server durability drill.

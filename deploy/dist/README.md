# Wyrd distribution tarball

This tarball installs the Wyrd roles on a bare-metal / VM Linux host with
systemd, per ADR-0010's substrate order (single binary primary, OCI image the
same binary, compose for eval). One `wyrd` binary serves every role as a
subcommand: `d-server`, `custodian`, `s3` (long-running, installed as units)
plus the `put` / `get` / `demo` CLI roles.

## What this build is

- **Flavor:** `fdb,etcd` — FoundationDB metadata (ADR-0042) + etcd coordination.
- **Linkage:** dynamically linked, glibc ≥ 2.36 (built on Debian bookworm; the
  binary is bit-identical to the one in the `wyrd:<version>-fdb` OCI image).
  An fdb-backed wyrd is *never* a single static binary — FoundationDB does not
  support static `libfdb_c` (architecture doc `07-deployment-view.md` §7.6).
- **Runtime requirement:** `libfdb_c` from `foundationdb-clients` at the exact
  pinned version (see `VERSION`); the installer prints the download command if
  it is missing. The version must match the FDB cluster you deploy against.

## Install

```sh
sudo ./install.sh                # or --prefix /opt/wyrd
```

The installer creates the `wyrd` system user, installs the binary to
`<prefix>/bin/wyrd`, the units to `/etc/systemd/system/`, per-role configs to
`/etc/wyrd/<role>.env` (from the bundled examples; existing configs are never
overwritten), and `/var/lib/wyrd`. It does **not** enable or start anything.

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

Re-run `install.sh` from the new tarball: binary, units, and `.example` files
are refreshed; your `/etc/wyrd/<role>.env` files are preserved. Restart the
units to pick up the new binary.

## Uninstall

```sh
sudo ./install.sh --uninstall            # keeps /etc/wyrd and /var/lib/wyrd
sudo ./install.sh --uninstall --purge    # deletes config AND fragment data
```

## Verify an install end-to-end

Unit sanity without a cluster: `systemd-analyze verify
/etc/systemd/system/wyrd-*.service`, and `wyrd` with no arguments prints usage.

A full three-role bring-up needs FDB + etcd + a D-server fleet. For a
single-host rehearsal, start the backends from the repo's
`deploy/small-multi-node-fdb/` compose fixture, point the env files at the
compose-published endpoints, then `systemctl start` the roles and drive an
object through: `wyrd put … --endpoints …` / `wyrd get …` (or an S3 client
against `wyrd-s3`). The repo's day-one runbook (m4-first-deployment-blueprint)
covers the kill-a-D-server durability drill.

#!/bin/sh
# wyrd @VERSION@ — installer for the bare-metal role deployment (ADR-0010:
# systemd on the storage hosts; the OCI image is the container path).
#
# Installs: the `wyrd` binary, systemd units for the three long-running roles
# (d-server, custodian, s3), and /etc/wyrd/<role>.env config files (from the
# bundled examples, NEVER overwriting an existing one). It does NOT enable or
# start anything — wiring a host into a cluster is an operator decision.
#
# Usage:
#   sudo ./install.sh [--prefix /usr/local]
#   sudo ./install.sh --uninstall [--purge --yes]
#
# Idempotent: re-running upgrades the binary, units, and .example files, and
# preserves live /etc/wyrd/<role>.env configs.
set -eu

PREFIX=/usr/local
UNINSTALL=0
PURGE=0
YES=0

while [ $# -gt 0 ]; do
    case "$1" in
        --prefix)
            [ $# -ge 2 ] || { echo "error: --prefix needs a value" >&2; exit 2; }
            PREFIX=$2
            shift 2
            ;;
        --uninstall) UNINSTALL=1; shift ;;
        --purge) PURGE=1; shift ;;
        --yes) YES=1; shift ;;
        -h|--help)
            sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "error: unknown argument \`$1\` (see --help)" >&2
            exit 2
            ;;
    esac
done

BINDIR=$PREFIX/bin
UNITDIR=/etc/systemd/system
CONFDIR=/etc/wyrd
DATADIR=/var/lib/wyrd
ROLES="d-server custodian s3"
HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)

# Validate the prefix BEFORE any mutation: refusing after the user/dirs/binary
# were created would leave a partial install with no units. A newline cannot
# ride through sed; a double quote cannot be represented inside the units'
# quoted ExecStart word; and `$` / `\` are rejected by systemd itself in an
# executable path ("Executable name contains special characters" — fatal at
# unit load, however escaped), so a unit pointing there could never start.
if [ "$BINDIR" != "$(printf '%s' "$BINDIR" | tr -d '\n"$' | tr -d "\\\\")" ]; then
    echo "error: --prefix must not contain a newline, double quote, \$, or backslash" >&2
    echo "       (systemd rejects such executable paths at unit load)" >&2
    exit 2
fi
case $PREFIX in
    /*) : ;;
    *)
        echo "error: --prefix must be an absolute path (units execute by absolute path)" >&2
        exit 2
        ;;
esac
# The units run with ProtectHome=yes: a binary under /home, /root, or /run/user
# would install fine and then be INVISIBLE to the service at exec time — refuse
# up front rather than hand the operator a unit that can never start.
case $PREFIX in
    /home/* | /home | /root/* | /root | /run/user/*)
        echo "error: --prefix under /home, /root, or /run/user is hidden from the services" >&2
        echo "       by their ProtectHome=yes hardening; use /usr/local or /opt instead" >&2
        exit 2
        ;;
esac

[ "$(id -u)" = 0 ] || { echo "error: must run as root (units, users, $CONFDIR)" >&2; exit 1; }

have_systemd() { command -v systemctl >/dev/null 2>&1; }
# systemd is INSTALLED vs RUNNING: in a container (or chroot) systemctl exists but
# no manager does, and daemon-reload would fail the whole install under `set -e`.
# /run/systemd/system is the canonical booted-with-systemd marker (sd_booted(3)).
systemd_running() { have_systemd && [ -d /run/systemd/system ]; }

if [ "$UNINSTALL" = 1 ]; then
    # Confirm the purge BEFORE any removal: asking after the units/binary are
    # gone would leave a declining operator half-uninstalled while being told
    # nothing happened.
    if [ "$PURGE" = 1 ] && [ "$YES" != 1 ]; then
        echo "--purge will DELETE $CONFDIR and $DATADIR, including any fragment data."
        printf 'Type "purge" to confirm: '
        read -r answer
        [ "$answer" = purge ] || { echo "aborted; nothing removed."; exit 1; }
    fi
    for role in $ROLES; do
        unit=wyrd-$role.service
        if systemd_running && [ -f "$UNITDIR/$unit" ]; then
            systemctl disable --now "$unit" 2>/dev/null || true
        fi
        rm -f "$UNITDIR/$unit"
    done
    rm -f "$BINDIR/wyrd"
    if systemd_running; then systemctl daemon-reload; fi
    if [ "$PURGE" = 1 ]; then
        rm -rf "$CONFDIR" "$DATADIR"
        echo "wyrd uninstalled ($BINDIR/wyrd, units); purged $CONFDIR and $DATADIR."
    else
        echo "wyrd uninstalled ($BINDIR/wyrd, units). Config and data kept:"
        echo "  $CONFDIR  $DATADIR"
    fi
    exit 0
fi

# ── install ──────────────────────────────────────────────────────────────────

# System user the units run as. Dynamic uid on bare metal; the OCI image's fixed
# uid 10001 never shares a filesystem with this host layout, so no need to match.
if ! getent passwd wyrd >/dev/null 2>&1; then
    useradd --system --user-group --no-create-home --shell /usr/sbin/nologin wyrd
fi

install -d -m 0755 "$BINDIR"
install -m 0755 "$HERE/bin/wyrd" "$BINDIR/wyrd"

install -d -m 0750 -o root -g wyrd "$CONFDIR"
install -d -m 0750 -o wyrd -g wyrd "$DATADIR"

for role in $ROLES; do
    # Examples are always refreshed; the LIVE config is created once and then
    # owned by the operator — an upgrade must never clobber it.
    install -m 0644 "$HERE/etc/$role.env.example" "$CONFDIR/$role.env.example"
    if [ ! -f "$CONFDIR/$role.env" ]; then
        install -m 0644 "$HERE/etc/$role.env.example" "$CONFDIR/$role.env"
    fi
done
# s3.env carries the gateway credentials — group-readable by the service user only.
chown root:wyrd "$CONFDIR/s3.env"
chmod 0640 "$CONFDIR/s3.env"

# Units ship with ExecStart="@BINDIR@/wyrd" …; substitute the REAL binary dir so
# a custom --prefix can never leave a unit pointing at a nonexistent path. The
# prefix is OPERATOR INPUT crossing TWO parsers (a newline / double quote was
# already refused up front, before any mutation), so escape for both:
#   1. systemd command-line syntax: the ExecStart word is double-quoted in the
#      unit templates (whitespace-safe), and `%` is a specifier — double it as
#      `%%`. (`$`, `\`, `"`, and newlines were REFUSED up front: systemd
#      rejects such executable paths at unit load, however escaped.)
#   2. sed replacement syntax: escape \ & and the | delimiter — an unescaped `&`
#      would expand back to `@BINDIR@`, a `|` would break the expression.
BINDIR_UNIT=$(printf '%s' "$BINDIR" | sed 's/%/%%/g')
BINDIR_ESCAPED=$(printf '%s' "$BINDIR_UNIT" | sed 's/[\\&|]/\\&/g')
for role in $ROLES; do
    sed "s|@BINDIR@|$BINDIR_ESCAPED|g" "$HERE/systemd/wyrd-$role.service" \
        >"$UNITDIR/wyrd-$role.service"
    chmod 0644 "$UNITDIR/wyrd-$role.service"
done
if systemd_running; then systemctl daemon-reload; fi

# libfdb_c preflight — WARN, don't refuse: staging an install before the FDB
# cluster exists is legitimate, and systemd gives a clear loader error at start.
if ! ldconfig -p 2>/dev/null | grep -q libfdb_c \
    && [ ! -e /usr/lib/libfdb_c.so ] && [ ! -e /usr/lib64/libfdb_c.so ]; then
    cat <<'EOF'

WARNING: libfdb_c not found. This build links FoundationDB at load time and
will not start without it (an fdb-backed wyrd is never a static binary).
Install the EXACT matching client package:

  curl -fsSLO https://github.com/apple/foundationdb/releases/download/@FDB_VERSION@/foundationdb-clients_@FDB_VERSION@-1_amd64.deb
  dpkg -i foundationdb-clients_@FDB_VERSION@-1_amd64.deb

EOF
fi

cat <<EOF
wyrd @VERSION@ installed:
  binary   $BINDIR/wyrd
  units    $UNITDIR/wyrd-{d-server,custodian,s3}.service   (installed, NOT enabled)
  config   $CONFDIR/<role>.env                              (edit before starting)
  data     $DATADIR                                         (StateDirectory parent)

Next steps, per role you want on THIS host:
  1. Edit $CONFDIR/<role>.env (see the .example alongside; the s3 credentials
     are REQUIRED, auth is fail-closed).
  2. systemctl enable --now wyrd-<role>

WARNING: run exactly ONE custodian per cluster — single-active is not enforced
against a distributed metadata store until the etcd Coordination backend (#365).
EOF

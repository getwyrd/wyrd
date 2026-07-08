# The Tier-1 fault agent: a minimal image whose only job is to run `iptables` INSIDE a
# target TiKV node's network namespace (`docker run --rm --privileged
# --network container:<node> wyrd-iptables:local <iptables args>`), applying/removing the
# symmetric partition rules of the M4.6 (#257) Tier-1 metadata consistency leg.
#
# Running the cut in the TARGET's netns (not on the host) is what makes the partition
# genuinely bidirectional for every packet the node sends or receives — and makes a leaked
# HOST firewall rule structurally impossible (Invariant B): the worst-case residue lives in
# the throwaway container's netns and dies with it.
#
# Built by the Tier-1 runner (`cargo xtask` metadata tier job) as `wyrd-iptables:local`.
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends iptables \
    && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["iptables"]

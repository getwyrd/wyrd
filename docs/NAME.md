# Why "Wyrd"

In Norse and Old English myth, *wyrd* is fate — not destiny as a fixed script,
but the woven web of what has happened, what is happening, and what is yet owed.
It is tended by the three Norns at the well beneath the world-tree: **Urðr**
(what has become), **Verðandi** (what is becoming), and **Skuld** (what shall be,
what is owed).

A storage system is, in the end, a keeper of wyrd. It holds what has been written
and made irrevocable, it weaves in what is being written now, and it carries the
debts of what it still owes — replicas not yet made, repairs not yet run. The
whole woven state is the **Wyrd**; the components that maintain it are named for
the Norns who weave it:

- **Urth** keeps what has become — the durable committed record.
- **Verdandi** handles what is becoming — the write and commit path.
- **Skuld** tracks what is owed — replication, repair, and reconciliation.

The name is also, cheerfully, a homophone of *weird* — which anyone who has
operated a distributed storage system at scale will agree is the correct word for
what happens at 3 a.m. We embrace it.

Wyrd stands in the lineage of Colossus-class storage systems — metadata and data
kept apart yet committed as one indivisible act — but it is its own thing: a
foundation that scales from a single machine to a fleet of datacenters, and
keeps, faithfully, whatever you weave into it.

---

*Naming and the component scheme are recorded as a permanent decision in
[ADR-0017](design/adr/0017-project-name-and-norn-scheme.md).*

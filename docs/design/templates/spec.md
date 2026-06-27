---
created: <DD.MM.YYYY HH:MM>
type: spec
status: draft   # draft | stable | superseded | withdrawn
version: v<N>
stability: unstable   # unstable | stable
tracking-issue:
supersedes:
tags:
  - spec
---
# Spec: <title> -- <version>

> Normative specification. **Status: DRAFT (unstable -- will change).** This
> document uses [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) keywords:
> MUST, SHOULD, MAY.

<!-- Specifications are normative, versioned contracts. They are stricter than
     architecture docs: once a spec version is stable, edits require a new
     version or a superseding spec. Number/path the file by domain and version,
     for example ../specs/<domain>/v<N>.md, and add conformance vectors whenever
     the contract affects bytes, wire behavior, or compatibility. -->

## Motivation

What contract needs to be written down? Why does it need to be stable across
implementations, deployments, upgrades, or time?

## Scope

What this specification covers and, just as importantly, what it does not cover.
Name adjacent specs, ADRs, proposals, and implementation-first areas.

## Conventions

Define terminology, units, encoding rules, ordering rules, RFC 2119 usage, and
any assumptions a conforming implementation relies on.

## Requirements

The normative contract. Be precise enough that an independent implementation can
conform without reading the reference code.

## Versioning and compatibility

What changes are compatible within this version? What changes require a version
bump? How do old data, old clients, or mixed-version deployments behave?

## Conformance

How does an implementation prove it conforms? Reference required test vectors,
property tests, fault-injection runs, or interoperability checks.

## Open questions

Mark unresolved points explicitly. A stable spec should not retain open
questions that affect compatibility or conformance.

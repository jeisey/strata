# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-06-18

### Added

- Initial release of the layered, signature-routed ETL framework.
- **Layer 0 (Index):** register source files with a generated GUID, original
  filename, content hash, byte size, and registration timestamp.
- **Layer 1 (Raw):** extract every sheet of a workbook (or a CSV) into generic
  2D string blobs, each with its own GUID and lineage back to the source file.
- **Layer 2 (Signature):** MD5 signature of the header row (line #1) plus
  schema metadata (column names, inferred types, row counts).
- **Layer 3 (Routing):** a `signature -> handler` lookup table and a handler
  registry that dynamically dispatches each blob to the correct processor.
- **Layer 4 (Semantic):** handlers extract typed records into output tables
  while preserving lineage back to the originating blob and source file.
- Pluggable `Storage` backend with a batteries-included SQLite implementation.
- Pluggable readers for CSV (stdlib) and Excel (`openpyxl`, optional extra).
- `strata` command-line interface for ingesting, inspecting, and processing.
- Worked `examples/prices` end-to-end demonstration and a full test suite.

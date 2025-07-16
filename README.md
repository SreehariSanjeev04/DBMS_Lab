# NITCbase

**NITCbase** is a simplified C++ implementation of a **relational database management system (RDBMS)**, built as part of the Database Management Systems Lab (Semester 4) at **NIT Calicut**.

It mimics fundamental DBMS operations like relational algebra, file-based record storage, indexing, and query processing — all without relying on external database libraries.

**Project Page:** [https://nitcbase.github.io](https://nitcbase.github.io)

---

## Features

- **Schema Management**  
  Define relations, attributes, and datatypes through schema files.

- **Record Storage**  
  Store and retrieve records using block-based file structures.

- **Relational Algebra Support**  
  Basic operations implemented:  
  - Selection (σ)  
  - Projection (π)  
  - Union (⋃)  
  - Cartesian Product (×)  
  - Rename (ρ)  
  - Join (⨝)

- **Indexing**  
  Efficient access using secondary indexing on specific attributes.

- **Query Processor**  
  Parse and evaluate user-defined queries using a CLI interface.

- **Disk Block Simulation**  
  All operations simulate disk blocks and pages with fixed sizes (e.g., 512 bytes).

---

## Concepts Covered

- Relational Model & Algebra  
- Block and Cache simulation  
- Indexing (secondary indexes)  
- Query evaluation  
- Record and attribute-level data handling  
- CLI design for command-based DB interaction

---

## How to Run

### Prerequisites

- A C++ compiler (e.g., `g++`)
- Linux/macOS terminal or WSL (for best compatibility)

### Compilation

```bash
g++ -std=c++17 -o nitcbase main.cpp relation.cpp block.cpp record.cpp index.cpp query_processor.cpp

# ercs

[![CI](https://github.com/dungeon2567/rollback_ecs/actions/workflows/ci.yml/badge.svg)](https://github.com/dungeon2567/rollback_ecs/actions/workflows/ci.yml)

## Description

Bitmask-based hierarchical ECS. Storages are organized in blocks (dense and sparse) that use `presence_mask` and `absence_mask` to represent entity membership and exclusion across recursive trees. Systems iterate views by intersecting masks, enabling fast per-block runs and efficient filtering.

### Highlights
- Hierarchical storage with dense and sparse blocks
- `presence_mask` and `absence_mask` for effective selection (`presence & !absence`)
- Run-time view intersection for multi-component systems
- Attribute macro `#[system]` to generate `System` structs from functions
- Typed `World::get<T>()` returning component storages

## Development

- Build: `cargo build`
- Test: `cargo test`

## CI

- GitHub Actions runs build and tests on `ubuntu-latest` using nightly toolchain.

//! Property-based tests for row encoding round-trips and page insert/get.
//!
//! Uses proptest to generate random schemas + matching rows and verifies:
//!   1. encode_row → decode_row produces the original values.
//!   2. Page::insert → Page::get returns the exact encoded bytes.

use powdb_storage::page::{Page, PageType};
use powdb_storage::row::{decode_row, encode_row};
use powdb_storage::types::*;

use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Generate an arbitrary Value for a given TypeId.
fn value_for_type(tid: TypeId) -> BoxedStrategy<Value> {
    match tid {
        TypeId::Int => any::<i64>().prop_map(Value::Int).boxed(),
        TypeId::Float => prop::num::f64::NORMAL.prop_map(Value::Float).boxed(),
        TypeId::Bool => any::<bool>().prop_map(Value::Bool).boxed(),
        TypeId::Str => "[a-zA-Z0-9 _]{0,100}".prop_map(Value::Str).boxed(),
        TypeId::DateTime => any::<i64>().prop_map(Value::DateTime).boxed(),
        TypeId::Uuid => prop::array::uniform16(any::<u8>())
            .prop_map(Value::Uuid)
            .boxed(),
        TypeId::Bytes => prop::collection::vec(any::<u8>(), 0..64)
            .prop_map(Value::Bytes)
            .boxed(),
        TypeId::Empty => Just(Value::Empty).boxed(),
    }
}

/// The set of TypeIds we can use in schemas (everything except Empty,
/// which is a value sentinel, not a column type).
fn type_id_strategy() -> impl Strategy<Value = TypeId> {
    prop_oneof![
        Just(TypeId::Int),
        Just(TypeId::Float),
        Just(TypeId::Bool),
        Just(TypeId::Str),
        Just(TypeId::DateTime),
        Just(TypeId::Uuid),
        Just(TypeId::Bytes),
    ]
}

/// Generate a schema (Vec<TypeId>) and a matching row (Vec<Value>).
/// Optionally sprinkles Empty values in to test null-bitmap handling.
fn schema_and_row() -> impl Strategy<Value = (Schema, Vec<Value>)> {
    prop::collection::vec(type_id_strategy(), 1..=16).prop_flat_map(|type_ids| {
        let schema = Schema {
            table_name: "proptest".into(),
            columns: type_ids
                .iter()
                .enumerate()
                .map(|(i, &tid)| ColumnDef {
                    name: format!("c{i}"),
                    type_id: tid,
                    required: false,
                    position: i as u16,
                })
                .collect(),
        };

        // For each column, either generate a matching value or Empty (50/50).
        let value_strats: Vec<BoxedStrategy<Value>> = type_ids
            .into_iter()
            .map(|tid| {
                prop_oneof![
                    8 => value_for_type(tid),
                    2 => Just(Value::Empty),
                ]
                .boxed()
            })
            .collect();

        (Just(schema), value_strats).prop_map(|(schema, values)| (schema, values))
    })
}

/// Generate a schema and row that are guaranteed to have only non-Empty
/// values (useful for stricter equality checks).
fn schema_and_full_row() -> impl Strategy<Value = (Schema, Vec<Value>)> {
    prop::collection::vec(type_id_strategy(), 1..=16).prop_flat_map(|type_ids| {
        let schema = Schema {
            table_name: "proptest_full".into(),
            columns: type_ids
                .iter()
                .enumerate()
                .map(|(i, &tid)| ColumnDef {
                    name: format!("c{i}"),
                    type_id: tid,
                    required: true,
                    position: i as u16,
                })
                .collect(),
        };

        let value_strats: Vec<BoxedStrategy<Value>> =
            type_ids.into_iter().map(value_for_type).collect();

        (Just(schema), value_strats)
    })
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    /// Row round-trip: encode → decode must recover the original values.
    #[test]
    fn row_encode_decode_roundtrip((schema, row) in schema_and_row()) {
        let encoded = encode_row(&schema, &row);

        // Length prefix should match buffer length.
        let stored_len = u16::from_le_bytes(encoded[0..2].try_into().unwrap()) as usize;
        prop_assert_eq!(stored_len, encoded.len());

        let decoded = decode_row(&schema, &encoded);
        prop_assert_eq!(decoded.len(), row.len());
        for (dec, orig) in decoded.iter().zip(row.iter()) {
            prop_assert_eq!(dec, orig);
        }
    }

    /// Same as above but with all non-Empty values.
    #[test]
    fn row_roundtrip_no_empties((schema, row) in schema_and_full_row()) {
        let encoded = encode_row(&schema, &row);
        let decoded = decode_row(&schema, &encoded);
        prop_assert_eq!(&decoded, &row);
    }

    /// Page insert/get round-trip: inserting encoded row bytes into a Page
    /// and retrieving them by slot must return identical bytes.
    #[test]
    fn page_insert_get_roundtrip(data in prop::collection::vec(any::<u8>(), 1..512)) {
        let mut page = Page::new(0, PageType::Data);
        if let Some(slot) = page.insert(&data) {
            let retrieved = page.get(slot).unwrap();
            prop_assert_eq!(retrieved, data.as_slice());
        }
        // If insert returns None, the data was too large — that's fine, skip.
    }

    /// Insert multiple rows into a page and verify each one comes back intact.
    #[test]
    fn page_multi_insert_roundtrip(rows in prop::collection::vec(
        prop::collection::vec(any::<u8>(), 1..128),
        1..32
    )) {
        let mut page = Page::new(1, PageType::Data);
        let mut inserted = Vec::new();

        for row in &rows {
            if let Some(slot) = page.insert(row) {
                inserted.push((slot, row.clone()));
            }
        }

        for (slot, expected) in &inserted {
            let actual = page.get(*slot).unwrap();
            prop_assert_eq!(actual, expected.as_slice());
        }
    }

    /// End-to-end: generate a schema + row, encode it, insert into a page,
    /// retrieve from page, decode, and verify equality with the original.
    #[test]
    fn full_roundtrip_schema_to_page((schema, row) in schema_and_row()) {
        let encoded = encode_row(&schema, &row);

        let mut page = Page::new(0, PageType::Data);
        if let Some(slot) = page.insert(&encoded) {
            let from_page = page.get(slot).unwrap();
            prop_assert_eq!(from_page, encoded.as_slice());

            let decoded = decode_row(&schema, from_page);
            prop_assert_eq!(decoded.len(), row.len());
            for (dec, orig) in decoded.iter().zip(row.iter()) {
                prop_assert_eq!(dec, orig);
            }
        }
    }
}

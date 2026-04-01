//! Test for the LabelEnum derive macro.

use ophanim::{DeriveLabel, LabelEnum, LabeledCounter};

#[derive(Copy, Clone, Debug, PartialEq, DeriveLabel)]
#[label_name = "method"]
enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    #[label = "other"]
    Unknown,
}

#[test]
fn test_cardinality() {
    assert_eq!(HttpMethod::CARDINALITY, 5);
}

#[test]
fn test_label_name() {
    assert_eq!(HttpMethod::LABEL_NAME, "method");
}

#[test]
fn test_as_index() {
    assert_eq!(HttpMethod::Get.as_index(), 0);
    assert_eq!(HttpMethod::Post.as_index(), 1);
    assert_eq!(HttpMethod::Put.as_index(), 2);
    assert_eq!(HttpMethod::Delete.as_index(), 3);
    assert_eq!(HttpMethod::Unknown.as_index(), 4);
}

#[test]
fn test_from_index() {
    assert_eq!(HttpMethod::from_index(0), HttpMethod::Get);
    assert_eq!(HttpMethod::from_index(1), HttpMethod::Post);
    assert_eq!(HttpMethod::from_index(2), HttpMethod::Put);
    assert_eq!(HttpMethod::from_index(3), HttpMethod::Delete);
    assert_eq!(HttpMethod::from_index(4), HttpMethod::Unknown);
    // Out of bounds returns last variant
    assert_eq!(HttpMethod::from_index(100), HttpMethod::Unknown);
}

#[test]
fn test_variant_name() {
    // Auto snake_case conversion
    assert_eq!(HttpMethod::Get.variant_name(), "get");
    assert_eq!(HttpMethod::Post.variant_name(), "post");
    assert_eq!(HttpMethod::Put.variant_name(), "put");
    assert_eq!(HttpMethod::Delete.variant_name(), "delete");
    // Custom label override
    assert_eq!(HttpMethod::Unknown.variant_name(), "other");
}

#[test]
fn test_label_name_method() {
    let method = HttpMethod::Get;
    assert_eq!(method.label_name(), "method");
}

#[test]
fn test_with_labeled_counter() {
    let counter: LabeledCounter<HttpMethod> = LabeledCounter::new(4);

    counter.inc(HttpMethod::Get);
    counter.add(HttpMethod::Post, 5);

    assert_eq!(counter.get(HttpMethod::Get), 1);
    assert_eq!(counter.get(HttpMethod::Post), 5);
    assert_eq!(counter.get(HttpMethod::Unknown), 0);
}

// Test PascalCase conversion for multi-word variants
#[derive(Copy, Clone, Debug, PartialEq, DeriveLabel)]
#[label_name = "error_type"]
enum ErrorType {
    NotFound,
    InternalServerError,
    BadRequest,
}

#[test]
fn test_pascal_to_snake_case() {
    assert_eq!(ErrorType::NotFound.variant_name(), "not_found");
    assert_eq!(
        ErrorType::InternalServerError.variant_name(),
        "internal_server_error"
    );
    assert_eq!(ErrorType::BadRequest.variant_name(), "bad_request");
}

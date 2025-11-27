use crate::component::{Component, Resource, Tag};

#[derive(Default, Clone)]
struct TestComponent1 {
    value: i32,
}

#[derive(Default, Clone)]
struct TestComponent2 {
    value: f32,
}

#[derive(Default, Clone)]
struct TestComponent3 {
    value: String,
}

#[derive(Default, Clone)]
struct TemporaryComponent {
    _value: u32,
}

impl Resource for TestComponent1 {
    fn type_index() -> usize {
        static TYPE_INDEX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *TYPE_INDEX.get_or_init(|| crate::component::next_id())
    }
}

impl Resource for TestComponent2 {
    fn type_index() -> usize {
        static TYPE_INDEX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *TYPE_INDEX.get_or_init(|| crate::component::next_id())
    }
}

impl Resource for TestComponent3 {
    fn type_index() -> usize {
        static TYPE_INDEX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *TYPE_INDEX.get_or_init(|| crate::component::next_id())
    }
}

impl Component for TestComponent1 {
    const IS_TEMPORARY: bool = false;
}

impl Component for TestComponent2 {
    const IS_TEMPORARY: bool = false;
}

impl Component for TestComponent3 {
    const IS_TEMPORARY: bool = false;
}

impl Component for TemporaryComponent {
    const IS_TEMPORARY: bool = true;
}

impl Resource for TemporaryComponent {
    fn type_index() -> usize {
        static TYPE_INDEX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *TYPE_INDEX.get_or_init(|| crate::component::next_id())
    }
}

#[derive(Default, Clone)]
struct TestTag1;

#[derive(Default, Clone)]
struct TestTag2;

impl Tag for TestTag1 {}
impl Tag for TestTag2 {}

impl Resource for TestTag1 {
    fn type_index() -> usize {
        static TYPE_INDEX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *TYPE_INDEX.get_or_init(|| crate::component::next_id())
    }
}

impl Resource for TestTag2 {
    fn type_index() -> usize {
        static TYPE_INDEX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *TYPE_INDEX.get_or_init(|| crate::component::next_id())
    }
}

#[test]
fn test_next_id_uniqueness() {
    let id1 = crate::component::next_id();
    let id2 = crate::component::next_id();
    let id3 = crate::component::next_id();

    assert_ne!(id1, id2);
    assert_ne!(id2, id3);
    assert_ne!(id1, id3);
}

#[test]
fn test_type_index_uniqueness() {
    let idx1 = TestComponent1::type_index();
    let idx2 = TestComponent2::type_index();
    let idx3 = TestComponent3::type_index();

    assert_ne!(idx1, idx2);
    assert_ne!(idx2, idx3);
    assert_ne!(idx1, idx3);
}

#[test]
fn test_type_index_consistency() {
    // Type index should be consistent across multiple calls
    let idx1 = TestComponent1::type_index();
    let idx2 = TestComponent1::type_index();
    let idx3 = TestComponent1::type_index();

    assert_eq!(idx1, idx2);
    assert_eq!(idx2, idx3);
}

#[test]
fn test_component_is_temporary_default() {
    // Default should be false
    assert_eq!(TestComponent1::IS_TEMPORARY, false);
    assert_eq!(TestComponent2::IS_TEMPORARY, false);
    assert_eq!(TestComponent3::IS_TEMPORARY, false);
}

#[test]
fn test_component_is_temporary_override() {
    // TemporaryComponent should override to true
    assert_eq!(TemporaryComponent::IS_TEMPORARY, true);
}

#[test]
fn test_destroyed_component() {
    use crate::component::Destroyed;

    // Destroyed should be a temporary component (it is just used as modification for other components)
    assert_eq!(Destroyed::IS_TEMPORARY, true);

    // Destroyed should have a type index
    let idx = Destroyed::type_index();
    assert!(idx < usize::MAX); // Just verify it's a valid index

    // Destroyed should be cloneable and default
    let destroyed1 = Destroyed::default();
    let destroyed2 = destroyed1.clone();
    // Just verify it compiles and can be used
    drop(destroyed1);
    drop(destroyed2);
}

#[test]
fn test_resource_clone() {
    let comp1 = TestComponent1 { value: 42 };
    let comp2 = comp1.clone();
    assert_eq!(comp1.value, comp2.value);
}

#[test]
fn test_resource_default() {
    let comp1 = TestComponent1::default();
    assert_eq!(comp1.value, 0);

    let comp2 = TestComponent2::default();
    assert_eq!(comp2.value, 0.0);

    let comp3 = TestComponent3::default();
    assert_eq!(comp3.value, "");
}

#[test]
fn test_tag_type_index() {
    let idx1 = TestTag1::type_index();
    let idx2 = TestTag2::type_index();

    assert_ne!(idx1, idx2);

    // Should be consistent
    assert_eq!(TestTag1::type_index(), idx1);
    assert_eq!(TestTag2::type_index(), idx2);
}

#[test]
fn test_component_any_trait() {
    // Verify components implement Any trait
    let comp1: Box<dyn std::any::Any> = Box::new(TestComponent1 { value: 42 });

    // Should be able to downcast
    assert!(comp1.downcast_ref::<TestComponent1>().is_some());
    assert!(comp1.downcast_ref::<TestComponent2>().is_none());
}

#[test]
fn test_resource_any_trait() {
    // Verify resources implement Any trait
    let comp1: Box<dyn std::any::Any> = Box::new(TestComponent1 { value: 42 });
    let comp2: Box<dyn std::any::Any> = Box::new(TestComponent2 { value: 3.14 });

    // Should be able to downcast correctly
    assert_eq!(comp1.downcast_ref::<TestComponent1>().unwrap().value, 42);
    assert_eq!(comp2.downcast_ref::<TestComponent2>().unwrap().value, 3.14);
    assert!(comp1.downcast_ref::<TestComponent2>().is_none());
    assert!(comp2.downcast_ref::<TestComponent1>().is_none());
}

#[test]
fn test_multiple_type_indices() {
    // Test that many different types get unique indices
    let indices: Vec<usize> = vec![
        TestComponent1::type_index(),
        TestComponent2::type_index(),
        TestComponent3::type_index(),
        TemporaryComponent::type_index(),
        TestTag1::type_index(),
        TestTag2::type_index(),
        crate::component::Destroyed::type_index(),
    ];

    // All should be unique
    for i in 0..indices.len() {
        for j in (i + 1)..indices.len() {
            assert_ne!(indices[i], indices[j], "Type indices should be unique");
        }
    }
}

#[test]
fn test_component_sized() {
    // Verify components are Sized (compile-time check)
    fn assert_sized<T: Sized>() {}

    assert_sized::<TestComponent1>();
    assert_sized::<TestComponent2>();
    assert_sized::<TestComponent3>();
    assert_sized::<TemporaryComponent>();
    assert_sized::<crate::component::Destroyed>();
}

#[test]
fn test_resource_sized() {
    // Verify resources are Sized (compile-time check)
    fn assert_sized<T: Sized>() {}

    assert_sized::<TestComponent1>();
    assert_sized::<TestTag1>();
}

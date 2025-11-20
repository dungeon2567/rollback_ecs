use std::any::TypeId;
use crate::world::World;

pub trait PipelineGroup: Sized + 'static {
    fn name(&self) -> &'static str where Self: 'static { std::any::type_name::<Self>() }
    fn instance() -> &'static Self where Self: Sized;
    fn type_id(&self) -> TypeId where Self: 'static { TypeId::of::<Self>() }
    fn before(&self) -> &'static [TypeId] { &[] }
    fn after(&self) -> &'static [TypeId] { &[] }
    fn parent(&self) -> Option<TypeId> { None }
}

pub trait PipelineStage: Sized + 'static {
    fn run(&self);
    fn name(&self) -> &'static str { std::any::type_name::<Self>() }
    fn type_id(&self) -> TypeId where Self: 'static { TypeId::of::<Self>() }
    fn before(&self) -> &'static [TypeId] { &[] }
    fn after(&self) -> &'static [TypeId] { &[] }
    fn reads(&self) -> &'static [TypeId] { &[] }
    fn writes(&self) -> &'static [TypeId] { &[] }
    fn create(world: &mut World) -> Self;
}

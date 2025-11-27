use crate::component::Component;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Entity(u32);

impl crate::component::Resource for Entity {
    fn type_index() -> usize {
        static TYPE_INDEX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *TYPE_INDEX.get_or_init(|| crate::component::next_id())
    }
}

impl Component for Entity {
    fn cleanup_system(world: &mut crate::world::World) -> Box<dyn crate::scheduler::PipelineStage> {
        use crate::scheduler::PipelineStage;
        Box::new(crate::system::DestroySystem::create(world))
    }
}

impl Entity {
    const GENERATION_BITS: u32 = 10;
    const INDEX_BITS: u32 = 22;
    const GENERATION_MASK: u32 = (1 << Self::GENERATION_BITS) - 1;
    const INDEX_MASK: u32 = (1 << Self::INDEX_BITS) - 1;

    #[inline(always)]
    /// Create a new Entity from index and generation
    pub fn new(index: u32, generation: u32) -> Self {
        let index = index & Self::INDEX_MASK;
        let generation = generation & Self::GENERATION_MASK;

        Entity((index << Self::GENERATION_BITS) | generation)
    }

    #[inline(always)]
    pub fn none() -> Self {
        Entity(0)
    }

    #[inline(always)]
    pub fn is_none(&self) -> bool {
        self.generation() == 0
    }

    #[inline(always)]
    pub fn index(&self) -> u32 {
        (self.0 >> Self::GENERATION_BITS) & Self::INDEX_MASK
    }

    #[inline(always)]
    pub fn set_index(&mut self, index: u32) {
        let index = index & Self::INDEX_MASK;
        self.0 = (self.0 & Self::GENERATION_MASK) | (index << Self::GENERATION_BITS);
    }

    #[inline(always)]
    /// Get the generation (lowest 8 bits)
    pub fn generation(&self) -> u32 {
        self.0 & Self::GENERATION_MASK
    }

    #[inline(always)]
    /// Set the generation (lowest 8 bits)
    pub fn set_generation(&mut self, generation: u32) {
        let generation = generation & Self::GENERATION_MASK;
        self.0 = (self.0 & !Self::GENERATION_MASK) | generation;
    }

    #[inline(always)]
    /// Increment the generation (wrapping)
    pub fn increment_generation(&mut self) {
        let generation = self.generation().wrapping_add(1) & Self::GENERATION_MASK;

        self.set_generation(if generation == 0 { 1u32 } else { generation });
    }
}

impl std::fmt::Debug for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Entity(id = {}, generation = {})",
            self.index(),
            self.generation()
        )
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Pos {
    generation: String,
    index: usize,
    offset: usize,
}

impl Pos {
    pub fn new(generation: String, index: usize, offset: usize) -> Self {
        Self {
            generation,
            index,
            offset,
        }
    }

    pub fn set_generation(&mut self, generation: String) {
        self.generation = generation;
        self.index = 0;
        self.offset = 0;
    }

    pub fn increment_index(&mut self) {
        self.index += 1;
        self.offset = 0;
    }

    pub fn increment_offset(&mut self, inc: usize) {
        self.offset += inc;
    }

    pub fn generation(&self) -> &str {
        &self.generation
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn index_str(&self) -> String {
        Self::format(self.index)
    }

    pub fn offset_str(&self) -> String {
        Self::format(self.offset)
    }

    pub fn format(value: usize) -> String {
        // TODO:
        // format!("{value:#016x}")
        format!("{value:0>16}")
    }
}

impl PartialOrd for Pos {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.generation.partial_cmp(&other.generation) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.index.partial_cmp(&other.index) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.offset.partial_cmp(&other.offset)
    }
}

impl Ord for Pos {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.generation.cmp(&other.generation) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        match self.index.cmp(&other.index) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        self.offset.cmp(&other.offset)
    }
}

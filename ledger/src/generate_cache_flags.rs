#[derive(Clone)]
pub struct GenerateCacheFlags {
    pub reps: bool,
    pub confirmed_count: bool,
    pub unchecked_count: bool,
    pub account_count: bool,
    pub block_count: bool,
}

impl GenerateCacheFlags {
    pub fn new() -> Self {
        Self {
            reps: true,
            confirmed_count: true,
            unchecked_count: true,
            account_count: true,
            block_count: true,
        }
    }

    pub fn enable_all(&mut self) {
        self.reps = true;
        self.confirmed_count = true;
        self.unchecked_count = true;
        self.account_count = true;
    }
}

impl Default for GenerateCacheFlags {
    fn default() -> Self {
        Self::new()
    }
}

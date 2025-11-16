#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use std::collections::HashMap;

use osw::PrecursorIdData;
use sqmass::TransitionGroup;

// Import necessary modules and dependencies
pub mod tsv;
pub mod osw;
pub mod oswpq;
pub mod sqmass;
pub mod compression;
pub mod util;
pub mod msnumpress;
pub mod xic_parquet;
 
pub trait ChromatogramReader: Send + Sync {
    fn new(db_path: &str) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn read_chromatograms(
        &self,
        filter_type: &str,
        filter_values: Vec<&str>,
        group_id: String,
    ) -> anyhow::Result<TransitionGroup>;

    fn read_chromatograms_for_precursors(
        &self,
        precursors: &[PrecursorIdData],
        include_precursor: bool,
        num_isotopes: usize,
    ) -> anyhow::Result<HashMap<i32, TransitionGroup>>;
}
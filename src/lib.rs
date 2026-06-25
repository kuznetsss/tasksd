// TODO: remove this
#![allow(dead_code)] // prevent too many warnings while developing
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
pub mod api;
pub mod application;
pub mod tasks;
pub mod transport;

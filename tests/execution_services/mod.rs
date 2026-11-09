// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests public submission and capacity-wait contracts.

use std::sync::mpsc;

pub(super) struct GateRelease(Option<mpsc::Sender<()>>);

impl GateRelease {
    pub(super) fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }

    pub(super) fn sender(&self) -> mpsc::Sender<()> {
        self.0.as_ref().expect("gate should not be released").clone()
    }
}

impl Drop for GateRelease {
    fn drop(&mut self) {
        self.release();
    }
}

mod submission_tests;
mod waiting_submission_tests;

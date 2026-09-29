// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Run business work, then let the application owner perform the final flush.
use std::error::Error;

use ioc_application_consumer::FlushWorker;
use ioc_application_consumer::build_application;
use qubit_execution_services::ExecutionServices;
use qubit_ioc::BuildFailure;
use qubit_ioc::ShutdownMode;
use tokio::runtime::Builder;

/// Keeps the runtime and temporary filesystem alive until all cleanup
/// completes.
fn main() -> Result<(), Box<dyn Error>> {
    let runtime = Builder::new_multi_thread().enable_all().build()?;
    let root = tempfile::tempdir()?;
    let application = match build_application(runtime.handle().clone(), root.path()) {
        Ok(application) => application,
        Err(error) => match error.downcast::<BuildFailure>() {
            Ok(failure) => {
                let (cause, cleanup) = failure.into_parts();
                eprintln!("Application construction failed: {cause}");
                if let Some(mut cleanup) = cleanup
                    && let Err(error) = runtime.block_on(cleanup.wait())
                {
                    eprintln!("Application cleanup failed: {error}; {:?}", error.report());
                }
                return Err(cause.into());
            }
            Err(error) => return Err(error),
        },
    };
    let business = (|| -> Result<_, Box<dyn Error>> {
        let context = application.context();
        let worker = context.get::<FlushWorker>()?;
        let services = context.get::<ExecutionServices>()?;
        let result = runtime.block_on(services.spawn_io(async { Ok::<u8, std::io::Error>(43) })?)?;
        assert_eq!(result, 43);
        Ok((worker, services))
    })();
    let mode = if business.is_ok() {
        ShutdownMode::Graceful
    } else {
        ShutdownMode::Immediate
    };
    let mut shutdown = application.begin_shutdown(mode);
    let cleanup = runtime.block_on(shutdown.wait());
    if let Err(error) = &cleanup {
        eprintln!("Application shutdown failed: {error}; {:?}", error.report());
    }
    let (worker, services) = business?;
    let report = cleanup?;
    assert!(report.is_success(), "{report:?}");
    assert_eq!(worker.messages(), ["final-report:22"]);
    assert_eq!(worker.final_task_result(), Some(22));
    assert!(services.is_terminated());
    Ok(())
}

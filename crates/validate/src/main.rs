#![forbid(unsafe_code)]
//! The `wyrd-validate` binary: a thin shell over the library that owns only the I/O
//! (proposal 0017 §2). It hands the real argv, the real environment and the real
//! stdout/stderr to [`wyrd_validate::run`], which makes every decision.

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // The raw value, not `std::env::var(..).ok()`: that folds "set but not UTF-8" into
    // "unset", and an unreadable AWS pair would then silently fall through to the Wyrd one.
    let lookup = |name: &str| std::env::var_os(name);
    let code = wyrd_validate::run(
        &args,
        &lookup,
        &mut std::io::stdout().lock(),
        &mut std::io::stderr().lock(),
    );
    ExitCode::from(code)
}

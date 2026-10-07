#![forbid(unsafe_code)]
//! The `wyrd-validate` binary: a thin shell over the library that owns only the I/O
//! (proposal 0017 §2). It hands the real argv, the real environment and the real
//! stdout/stderr to [`wyrd_validate::run`], which makes every decision.

use std::io::Write;
use std::process::ExitCode;

fn main() -> ExitCode {
    // `args_os`, not `args`: `std::env::args` panics on an argument that is not UTF-8, and a
    // path given to `--out` can legitimately be one. Refuse it as a usage error instead, so
    // every invocation ends in a status the operator can act on.
    let mut args = Vec::new();
    for (position, arg) in std::env::args_os().skip(1).enumerate() {
        match arg.into_string() {
            Ok(arg) => args.push(arg),
            Err(raw) => {
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "wyrd-validate: argument {} is not valid UTF-8 ({})\n{}",
                    position + 1,
                    raw.to_string_lossy(),
                    wyrd_validate::usage()
                );
                return ExitCode::from(wyrd_validate::EXIT_USAGE);
            }
        }
    }
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

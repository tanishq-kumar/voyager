//! `voy`: the short alias for the `voyager` command. It runs the same program.

#[path = "../main.rs"]
mod voyager;

fn main() -> std::process::ExitCode {
    voyager::main()
}

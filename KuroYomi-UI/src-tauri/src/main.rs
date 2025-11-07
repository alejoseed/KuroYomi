// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    std::env::set_var("__GL_THREADED_OPTIMIZATIONS", "0");
    std::env::set_var("__NV_DISABLE_EXPLICIT_SYNC", "1");
    kuroyomi_ui_lib::run()
}

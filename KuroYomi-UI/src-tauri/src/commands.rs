use crate::dictionary::loader::loader;

#[tauri::command]
pub fn load_dict() {
    loader();
}
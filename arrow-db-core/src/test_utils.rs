pub(crate) fn get_temp_dir(dir: &str) -> String {
    std::env::temp_dir().join(dir).to_str().unwrap().to_string()
}

pub(crate) async fn create_temp_dir(dir: &str) -> String {
    let temp_dir = get_temp_dir(dir);
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();

    temp_dir
}

pub(crate) async fn remove_temp_dir(temp_dir: &str) {
    tokio::fs::remove_dir_all(temp_dir).await.unwrap();
}

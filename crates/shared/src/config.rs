/// Runtime configuration, read from the environment. Defaults target the local
/// `docker compose` stack so `cargo run` works with no setup; on Railway the
/// platform injects `DATABASE_URL` / `REDIS_URL` / `PORT`.
#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    pub redis_url: String,
    pub bind_addr: String,
    /// Identifies this process in logs and as the matcher leader-lock token.
    pub instance_id: String,
}

impl Config {
    pub fn from_env() -> Self {
        // Railway exposes the listen port as PORT; fall back to BIND_ADDR/8080.
        let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| {
            let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
            format!("0.0.0.0:{port}")
        });
        Self {
            database_url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://eterna:eterna@localhost:5432/eterna".to_string()),
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://localhost:6379".to_string()),
            bind_addr,
            instance_id: std::env::var("INSTANCE_ID")
                .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string()),
        }
    }
}

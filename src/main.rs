//! ClipperOps Core — Motor de Agendamiento con Grafo de Relaciones
//!
//! Stack: Axum 0.8 · SurrealDB 2 (Graph) · Tracing estructurado · Tokio
//!
//! Arquitectura de capas:
//!   HTTP Handler → AppState (Repositorio) → SurrealDB
//!
//! Para correr: RUST_LOG=debug cargo run

// ─── IMPORTS ──────────────────────────────────────────────────────────────────

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use surrealdb::{
    engine::local::{Db, Mem},
    sql::Thing,
    Surreal,
};
use thiserror::Error;
use tracing::{error, info, instrument, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// ═══════════════════════════════════════════════════════════════════════════════
// CAPA DE ERROR
// Un solo tipo de error para toda la app. Axum lo convierte en HTTP automático.
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Error de base de datos: {0}")]
    Db(#[from] surrealdb::Error),

    #[error("Recurso no encontrado")]
    NotFound,

    #[error("Entrada inválida: {0}")]
    InvalidInput(String),

    #[error("Error interno: {0}")]
    Internal(String),
}

/// Convierte AppError → respuesta HTTP con status code correcto.
/// Los handlers retornan `AppResult<T>` y Axum hace el resto.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, mensaje) = match &self {
            AppError::Db(e) => {
                error!(error = %e, "Error en base de datos");
                (StatusCode::INTERNAL_SERVER_ERROR, "Error interno del servidor".to_string())
            }
            AppError::NotFound => (StatusCode::NOT_FOUND, "Recurso no encontrado".to_string()),
            AppError::InvalidInput(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg.clone()),
            AppError::Internal(msg) => {
                error!("Error interno: {}", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, "Error interno".to_string())
            }
        };

        (status, Json(ErrorBody { error: mensaje, codigo: status.as_u16() })).into_response()
    }
}

/// Alias de Result usado en toda la app.
type AppResult<T> = Result<T, AppError>;

// ═══════════════════════════════════════════════════════════════════════════════
// MODELOS DE DOMINIO
// Estos representan la verdad del negocio. Separados de los DTOs de la API.
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Cliente {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Thing>,
    pub nombre: String,
    pub telefono: String,
    pub frecuencia_estimada_dias: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Turno {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Thing>,
    pub fecha_hora: String,  // ISO-8601. En producción: chrono::DateTime<Utc>
    pub estado: EstadoTurno,
}

/// Enum en lugar de String crudo.
/// Si escribís "Pendente" en vez de "Pendiente" → error en compilación, no en producción.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EstadoTurno {
    Pendiente,
    Confirmado,
    Completado,
    Cancelado,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Agendo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Thing>,
    pub r#in: Thing,
    pub out: Thing,
    pub metodo: MetodoAgendamiento,
}

/// Enum tipado en lugar de "automatizacion_ia_zero_touch" hardcodeado como String.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum MetodoAgendamiento {
    AutomatizacionIaZeroTouch,
    WhatsappBot,
    Manual,
}

// ═══════════════════════════════════════════════════════════════════════════════
// DTOs — Contratos de la API (separados del dominio interno)
// Lo que entra y sale por HTTP no tiene por qué ser igual a tu modelo de datos.
// ═══════════════════════════════════════════════════════════════════════════════

/// Payload para crear un cliente via POST /clientes
#[derive(Debug, Deserialize)]
pub struct CrearClienteRequest {
    pub nombre: String,
    pub telefono: String,
    #[serde(default = "frecuencia_default")]
    pub frecuencia_estimada_dias: u32,
}

fn frecuencia_default() -> u32 {
    30
}

impl CrearClienteRequest {
    /// Validación de dominio. Falla rápido antes de tocar la DB.
    fn validar(&self) -> AppResult<()> {
        if self.nombre.trim().is_empty() {
            return Err(AppError::InvalidInput("`nombre` no puede estar vacío".into()));
        }
        if self.telefono.trim().is_empty() {
            return Err(AppError::InvalidInput("`telefono` no puede estar vacío".into()));
        }
        if !self.telefono.starts_with('+') {
            return Err(AppError::InvalidInput(
                "`telefono` requiere código internacional: +54...".into(),
            ));
        }
        if !(1..=365).contains(&self.frecuencia_estimada_dias) {
            return Err(AppError::InvalidInput(
                "`frecuencia_estimada_dias` debe estar entre 1 y 365".into(),
            ));
        }
        Ok(())
    }
}

// ─── Response wrappers ────────────────────────────────────────────────────────

/// Wrapper estándar para todas las respuestas exitosas.
#[derive(Serialize)]
struct ApiOk<T: Serialize> {
    data: T,
    mensaje: &'static str,
}

/// Wrapper estándar para todos los errores.
#[derive(Serialize)]
struct ErrorBody {
    error: String,
    codigo: u16,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    servicio: &'static str,
}

// ═══════════════════════════════════════════════════════════════════════════════
// ESTADO & REPOSITORIO
// AppState es la capa de acceso a datos. Toda lógica de DB vive acá, no en los handlers.
// ═══════════════════════════════════════════════════════════════════════════════

pub struct AppState {
    pub db: Surreal<Db>,
}

impl AppState {
    /// Crea un cliente con ID determinístico derivado del teléfono.
    /// Ventaja: idempotente — el mismo teléfono siempre produce el mismo ID.
    ///
    /// FIX DEL BUG ORIGINAL:
    /// `.create("tabla")` → SurrealDB auto-genera ID → retorna Vec<T>  ← ERROR
    /// `.create(("tabla", "id"))` → ID explícito → retorna Option<T>   ← CORRECTO
    pub async fn crear_cliente(&self, req: &CrearClienteRequest) -> AppResult<Cliente> {
        let id = telefono_a_id(&req.telefono);

        let cliente = Cliente {
            id: None,
            nombre: req.nombre.trim().to_string(),
            telefono: req.telefono.trim().to_string(),
            frecuencia_estimada_dias: req.frecuencia_estimada_dias,
        };

        let creado: Option<Cliente> = self.db
            .create(("cliente", id))   // ← ID explícito = Option<T>, no Vec<T>
            .content(cliente)
            .await?;

        creado.ok_or_else(|| AppError::Internal("La DB no retornó el cliente creado".into()))
    }

    pub async fn listar_clientes(&self) -> AppResult<Vec<Cliente>> {
        let clientes: Vec<Cliente> = self.db.select("cliente").await?;
        Ok(clientes)
    }
}

/// "+541155556666" → "541155556666" (válido como ID en SurrealDB)
fn telefono_a_id(tel: &str) -> String {
    tel.chars().filter(|c| c.is_alphanumeric()).collect()
}

// ═══════════════════════════════════════════════════════════════════════════════
// HANDLERS HTTP
// Solo orquestan: validan input, llaman al repositorio, formatean output.
// Cero lógica de negocio acá.
// ═══════════════════════════════════════════════════════════════════════════════

async fn health() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        servicio: "ClipperOps Core",
    })
}

/// POST /clientes
/// Crea un nuevo cliente desde un payload JSON.
#[instrument(skip(state), fields(nombre = %payload.nombre, tel = %payload.telefono))]
async fn crear_cliente(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CrearClienteRequest>,
) -> AppResult<impl IntoResponse> {
    payload.validar()?;
    info!("Creando cliente");

    let cliente = state.crear_cliente(&payload).await?;
    info!(id = ?cliente.id, "Cliente creado exitosamente");

    Ok((
        StatusCode::CREATED,
        Json(ApiOk { data: cliente, mensaje: "Cliente creado" }),
    ))
}

/// GET /clientes
/// Lista todos los clientes registrados.
#[instrument(skip(state))]
async fn listar_clientes(
    State(state): State<Arc<AppState>>,
) -> AppResult<impl IntoResponse> {
    let clientes = state.listar_clientes().await?;
    info!(total = clientes.len(), "Clientes listados");
    Ok(Json(ApiOk { data: clientes, mensaje: "OK" }))
}

// ═══════════════════════════════════════════════════════════════════════════════
// ROUTER
// Punto único de definición de rutas. Fácil de leer y de extender.
// ═══════════════════════════════════════════════════════════════════════════════

fn build_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health",   get(health))
        .route("/clientes", post(crear_cliente).get(listar_clientes))
        .with_state(state)
}

// ═══════════════════════════════════════════════════════════════════════════════
// SEED DE DESARROLLO
// Datos de prueba, aislados del main(). Solo se llama en dev.
// ═══════════════════════════════════════════════════════════════════════════════

async fn seed_dev(db: &Surreal<Db>) -> surrealdb::Result<()> {
    info!("Seeding datos de desarrollo...");

    let cliente: Option<Cliente> = db
        .create(("cliente", "matias_cardozo"))
        .content(Cliente {
            id: None,
            nombre: "Matias Cardozo (VIP)".into(),
            telefono: "+541100000000".into(),
            frecuencia_estimada_dias: 15,
        })
        .await?;

    let turno: Option<Turno> = db
        .create(("turno", "t_1"))
        .content(Turno {
            id: None,
            fecha_hora: "2026-05-14T18:00:00-03:00".into(),
            estado: EstadoTurno::Pendiente,
        })
        .await?;

    // Crear la relación en el grafo solo si ambos nodos existen
    if let (Some(c), Some(t)) = (&cliente, &turno) {
        let cid = c.id.clone().expect("El cliente recién creado debe tener ID");
        let tid = t.id.clone().expect("El turno recién creado debe tener ID");

        // SET en lugar de CONTENT para evitar conflictos con los campos `in`/`out` del edge
        let mut res = db
            .query("RELATE $in->agendo->$out SET metodo = $metodo")
            .bind(("in", cid))
            .bind(("out", tid))
            .bind(("metodo", MetodoAgendamiento::AutomatizacionIaZeroTouch))
            .await?;

        let relacion: Option<Agendo> = res.take(0)?;
        info!("Relación grafo creada: {:#?}", relacion);
    }

    info!("Seed completo. Cliente={:#?} | Turno={:#?}", cliente, turno);
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// Responsabilidad única: arrancar la infraestructura y servir.
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
    // Logging estructurado. Configurable via variable de entorno:
    // RUST_LOG=debug cargo run
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "clipper_ops_core=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Iniciando ClipperOps Core...");

    // Base de datos en memoria (cambiar a RocksDB para persistencia real)
    let db = Surreal::new::<Mem>(()).await?;
    db.use_ns("clipperops").use_db("produccion").await?;

    // Seed solo en desarrollo. En producción, usar migraciones.
    seed_dev(&db).await?;

    let state = Arc::new(AppState { db });
    let app = build_app(state);

    // Dirección configurable via variable de entorno
    let addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string());
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    info!("Servidor en http://{}", addr);

    // Graceful shutdown: espera que las conexiones activas terminen antes de cerrar
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
            warn!("Señal Ctrl+C recibida. Cerrando limpiamente...");
        })
        .await
        .unwrap();

    info!("Servidor detenido.");
    Ok(())
}
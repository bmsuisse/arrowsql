//! The public Rust API: connect, execute, fetch Arrow record batches.

use std::fmt;
use std::sync::{Arc, Weak};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use mssql_tds::datatypes::sqltypes::SqlType;
use parking_lot::Mutex;

use crate::arrow::plan::Plan;
use crate::arrow::sink::ArrowSink;
use crate::arrow::stream::{StreamCore, StreamReader};
use crate::connection::{ConnShared, PendingSession};
use crate::consts::*;
use crate::error::Error;
use crate::params::ParamSet;
use crate::runtime::Lockable;
use crate::statement::{self, BatchSink, Command, StmtShared};

/// Hands out Entra ID access tokens for connections that log in with one
/// (the Python bindings bridge their `token_factory` callable to this).
pub trait TokenProvider: Send + Sync {
    /// The access token (the JWT) and its expiry as Unix seconds (0: unknown).
    fn token(&self) -> Result<(String, i64), Error>;
}

/// Answers the federated-authentication step of a login: the server names
/// the service principal (`spn`) and the token service (`sts_url`, which
/// carries the tenant) mid-login. Returns the token as the login sends it
/// (UTF-16LE JWT bytes). Called on a blocking thread of the shared runtime.
pub trait FedAuthProvider: Send + Sync {
    fn token(&self, spn: &str, sts_url: &str, auth_method: &str) -> Result<Vec<u8>, Error>;
}

/// How to open a [`Connection`].
#[derive(Clone)]
pub struct ConnectOptions {
    /// ODBC-style connection string (`Server=...;Database=...;Uid=...`).
    pub conn_str: String,
    /// An Entra ID access token to log in with.
    pub access_token: Option<String>,
    /// Asked for an access token at each connect (instead of `access_token`).
    pub token_provider: Option<Arc<dyn TokenProvider>>,
    /// Asked mid-login for `Authentication=ActiveDirectory*` methods that
    /// need the server's STS URL.
    pub fedauth_provider: Option<Arc<dyn FedAuthProvider>>,
    /// Login timeout in seconds (default: the connection string's, else 15).
    pub login_timeout: Option<u32>,
    /// Each statement commits on its own (the default). Without it, a
    /// transaction is opened before the first statement and rolled back on
    /// close unless committed.
    pub autocommit: bool,
}

impl ConnectOptions {
    pub fn new(conn_str: impl Into<String>) -> ConnectOptions {
        ConnectOptions {
            conn_str: conn_str.into(),
            access_token: None,
            token_provider: None,
            fedauth_provider: None,
            login_timeout: None,
            autocommit: true,
        }
    }
}

/// Never prints the connection string or a token: both carry secrets.
impl fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectOptions")
            .field(
                "access_token",
                &self.access_token.as_ref().map(|_| "<redacted>"),
            )
            .field("token_provider", &self.token_provider.is_some())
            .field("fedauth_provider", &self.fedauth_provider.is_some())
            .field("login_timeout", &self.login_timeout)
            .field("autocommit", &self.autocommit)
            .finish_non_exhaustive()
    }
}

/// A parameter value for `?` placeholders.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Param {
    /// A NULL whose type the server infers from the statement.
    Null,
    Bool(bool),
    /// Bound as the narrowest of tinyint, smallint, int and bigint.
    I64(i64),
    F64(f64),
    /// ASCII text binds as varchar, other text as nvarchar.
    Str(String),
    Bytes(Vec<u8>),
    /// An mssql-tds value, for types the variants above do not cover.
    #[doc(hidden)]
    Sql(SqlType),
}

impl Param {
    fn sql_type(&self) -> SqlType {
        match self {
            Param::Null => SqlType::NVarchar(None, 1),
            Param::Bool(v) => SqlType::Bit(Some(*v)),
            Param::I64(v) => crate::params::int_sql_type(*v),
            Param::F64(v) => SqlType::Float(Some(*v)),
            Param::Str(v) => crate::params::str_sql_type(v.clone()),
            Param::Bytes(v) => match u16::try_from(v.len().max(1)) {
                Ok(length) if length <= 8_000 => SqlType::VarBinary(Some(v.clone()), length),
                _ => SqlType::VarBinaryMax(Some(v.clone())),
            },
            Param::Sql(v) => v.clone(),
        }
    }
}

/// The parameters of a parameterized command, with SQL Server's `@Pn`
/// placeholders in place of `?`.
pub(crate) fn command(sql: &str, params: &[Param]) -> Result<Command, Error> {
    if params.is_empty() {
        return Ok(Command::Batch(sql.to_string()));
    }
    let (sql, markers) = crate::params::placeholders::rewrite_qmarks(sql);
    if markers != params.len() {
        return Err(Error::driver(
            "07002",
            format!(
                "COUNT field incorrect or syntax error: the SQL contains {markers} parameter markers, but {} parameters were supplied",
                params.len()
            ),
        ));
    }
    let untyped_nulls = params
        .iter()
        .enumerate()
        .filter(|(_, p)| matches!(p, Param::Null))
        .map(|(i, _)| i)
        .collect();
    Ok(Command::Parameterized(
        sql,
        ParamSet {
            types: params.iter().map(Param::sql_type).collect(),
            untyped_nulls,
        },
    ))
}

/// Cancels a running execution from another thread.
#[derive(Clone)]
pub struct CancelHandle(Arc<dyn Fn() + Send + Sync>);

impl CancelHandle {
    pub(crate) fn new(cancel: impl Fn() + Send + Sync + 'static) -> CancelHandle {
        CancelHandle(Arc::new(cancel))
    }

    pub fn cancel(&self) {
        (self.0)()
    }
}

impl fmt::Debug for CancelHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CancelHandle")
    }
}

/// A session's owner: closes it once the connection and every statement and
/// reader made from it are gone.
pub(crate) struct ConnInner {
    pub(crate) shared: Arc<ConnShared>,
    /// The statement executing (or last executed) on this connection.
    current: Mutex<Weak<StmtShared>>,
    /// Closes a session that belongs to someone else (the Python bindings).
    on_close: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Drop for ConnInner {
    fn drop(&mut self) {
        if let Some(close) = self.on_close.lock().take() {
            close();
            return;
        }
        crate::connection::close_session(&self.shared);
    }
}

/// One TDS session. Cheap to clone (a handle); the session closes when the
/// last clone, statement and reader are dropped.
#[derive(Clone)]
pub struct Connection(pub(crate) Arc<ConnInner>);

impl Connection {
    /// Log in. Without `access_token`/`token_provider` the connection string
    /// says how (SQL login, integrated, `Authentication=ActiveDirectory*`).
    pub fn connect(options: &ConnectOptions) -> Result<Connection, Error> {
        let token = match (&options.token_provider, &options.access_token) {
            (Some(provider), _) => Some(provider.token()?.0),
            (None, token) => token.clone(),
        };
        let client = crate::connection::open_client(
            &options.conn_str,
            token,
            options.fedauth_provider.clone(),
            options.login_timeout,
        )
        .map_err(Error::from_records)?;
        let shared = ConnShared::new(client, options.autocommit, 0, PendingSession::default());
        Ok(Connection::from_shared(shared, None))
    }

    /// A connection over a session opened elsewhere. `on_close` runs instead
    /// of closing the session when the last handle is dropped.
    #[doc(hidden)]
    pub fn from_shared(
        shared: Arc<ConnShared>,
        on_close: Option<Box<dyn FnOnce() + Send>>,
    ) -> Connection {
        Connection(Arc::new(ConnInner {
            shared,
            current: Mutex::new(Weak::new()),
            on_close: Mutex::new(on_close),
        }))
    }

    #[doc(hidden)]
    pub fn shared(&self) -> &Arc<ConnShared> {
        &self.0.shared
    }

    /// Run `sql` (with `?` placeholders for `params`) and position on its
    /// first result. Blocks until the server answered.
    pub fn execute(&self, sql: &str, params: &[Param]) -> Result<Statement, Error> {
        self.execute_on(self.new_statement(), sql, params)
    }

    /// A statement handle on this connection, for [`execute_on`](Self::execute_on):
    /// it can be cancelled before the execution starts.
    #[doc(hidden)]
    pub fn new_statement(&self) -> Arc<StmtShared> {
        StmtShared::new(self.0.shared.clone())
    }

    #[doc(hidden)]
    pub fn execute_on(
        &self,
        stmt: Arc<StmtShared>,
        sql: &str,
        params: &[Param],
    ) -> Result<Statement, Error> {
        let command = command(sql, params)?;
        *self.0.current.lock() = Arc::downgrade(&stmt);
        let ret = crate::runtime::watching_if_enabled(&stmt, || {
            statement::execute_command(&stmt, command)
        });
        let statement = Statement {
            stmt,
            conn: self.0.clone(),
            plan: None,
            detached: false,
        };
        if ret == SQL_ERROR || ret == SQL_INVALID_HANDLE {
            return Err(Error::from_records(statement::diag_records(
                &statement.stmt,
            )));
        }
        Ok(statement)
    }

    /// Cancels whatever this connection is executing when called (a
    /// statement started later is not affected).
    pub fn cancel_handle(&self) -> CancelHandle {
        let inner = Arc::downgrade(&self.0);
        CancelHandle(Arc::new(move || {
            if let Some(stmt) = inner.upgrade().and_then(|c| c.current.lock().upgrade()) {
                stmt.cancel_execution();
            }
        }))
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Connection")
    }
}

/// The results of one execution. Dropping it discards unread rows.
pub struct Statement {
    stmt: Arc<StmtShared>,
    conn: Arc<ConnInner>,
    /// The Arrow plan of the current result and its generation.
    plan: Option<(Plan, u64)>,
    /// Handed to a reader: dropping the statement must not close its result.
    detached: bool,
}

/// A scalar of a small metadata result (see [`Statement::scalar_rows`]).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Value {
    Null,
    Int(i64),
    Bool(bool),
    Text(String),
    /// Any other type.
    Other,
}

impl Value {
    /// Python's view of the value as an `int` (bool included), if it is one.
    pub(crate) fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            Value::Bool(v) => Some(i64::from(*v)),
            _ => None,
        }
    }

    /// Python's truth value of a bool/int column.
    pub(crate) fn truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Int(v) => *v != 0,
            Value::Bool(v) => *v,
            Value::Text(v) => !v.is_empty(),
            Value::Other => true,
        }
    }
}

impl Statement {
    pub(crate) fn shared_stmt(&self) -> &Arc<StmtShared> {
        &self.stmt
    }

    /// Up to `max_rows` rows of the current result as plain values.
    pub(crate) fn scalar_rows(&mut self, max_rows: usize) -> Result<Vec<Vec<Value>>, Error> {
        use crate::decode::buffer::Cell;
        use crate::statement::cursor::{FetchPlan, fetch};
        let mut out = Vec::new();
        while out.len() < max_rows {
            let fetched = fetch(&self.stmt, max_rows - out.len(), FetchPlan::Batch);
            match fetched.ret {
                SQL_NO_DATA => break,
                SQL_ERROR | SQL_INVALID_HANDLE => {
                    return Err(Error::from_records(statement::diag_records(&self.stmt)));
                }
                _ => {}
            }
            let rows = &fetched.rows;
            let ncols = fetched.ncols.max(1);
            for row in rows.cells.chunks(ncols).take(rows.rows) {
                out.push(
                    row.iter()
                        .map(|cell| match cell {
                            Cell::Null => Value::Null,
                            Cell::Int(v) => Value::Int(*v),
                            Cell::Bool(v) => Value::Bool(*v),
                            Cell::Utf8(_) | Cell::Utf16(_) => {
                                rows.text(cell).map_or(Value::Other, Value::Text)
                            }
                            _ => Value::Other,
                        })
                        .collect(),
                );
            }
        }
        Ok(out)
    }

    fn plan(&mut self) -> Option<&Plan> {
        let (columns, generation, _) = statement::result_columns(&self.stmt)?;
        if self.plan.as_ref().is_none_or(|(_, g)| *g != generation) {
            self.plan = Some((Plan::new(&columns).ok()?, generation));
        }
        self.plan.as_ref().map(|(plan, _)| plan)
    }

    /// The Arrow schema of the current result; `None` when it has no rows
    /// (DML, DDL) or its columns have no Arrow mapping.
    pub fn schema(&mut self) -> Option<SchemaRef> {
        self.plan().map(|plan| plan.schema.clone())
    }

    /// Rows affected by the current result (`-1`: not reported).
    pub fn rows_affected(&self) -> i64 {
        self.stmt.state.lock_checked().rowcount()
    }

    /// Up to `max_rows` rows of the current result; `None` at its end.
    pub fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>, Error> {
        let max_rows = max_rows.max(1);
        if statement::result_columns(&self.stmt).is_none() {
            return Ok(None);
        }
        let stmt = self.stmt.clone();
        let Some(plan) = self.plan() else {
            return Err(Error::driver(
                "HY000",
                "the result has columns without an Arrow mapping",
            ));
        };
        let mut sink = ArrowSink::new(plan, max_rows);
        let fetched = statement::fetch_into(&stmt, &mut sink, max_rows, None);
        match fetched.ret {
            SQL_ERROR | SQL_INVALID_HANDLE => {
                Err(Error::from_records(statement::diag_records(&stmt)))
            }
            SQL_NO_DATA => Ok(None),
            _ if sink.rows() == 0 => Ok(None),
            _ => sink
                .finish()
                .map(Some)
                .map_err(|e| Error::driver("HY000", e)),
        }
    }

    /// The rest of the current result, as batches of `batch_rows` rows,
    /// fetched on the calling thread.
    pub fn read_all(&mut self, batch_rows: usize) -> Result<(SchemaRef, Vec<RecordBatch>), Error> {
        let (plan, ..) = crate::arrow::current_plan(&self.stmt).map_err(Error::state_error)?;
        let fetched = crate::arrow::fetch_table(&self.stmt, &plan, usize::MAX, batch_rows);
        match fetched.ret {
            Err(message) => Err(Error::driver("HY000", message)),
            Ok(SQL_SUCCESS | SQL_SUCCESS_WITH_INFO) => Ok((plan.schema, fetched.batches)),
            Ok(_) => Err(Error::from_records(statement::diag_records(&self.stmt))),
        }
    }

    /// Move to the next result; false when there is none.
    pub fn next_result(&mut self) -> Result<bool, Error> {
        match statement::cursor::next_result(&self.stmt) {
            SQL_NO_DATA => Ok(false),
            SQL_ERROR | SQL_INVALID_HANDLE => {
                Err(Error::from_records(statement::diag_records(&self.stmt)))
            }
            _ => Ok(true),
        }
    }

    /// Cancels this statement's execution or fetch from another thread.
    pub fn cancel_handle(&self) -> CancelHandle {
        let stmt = Arc::downgrade(&self.stmt);
        CancelHandle(Arc::new(move || {
            if let Some(stmt) = stmt.upgrade() {
                stmt.cancel_execution();
            }
        }))
    }

    /// The rest of the current result as a reader whose batches are decoded
    /// ahead on a background thread (`prefetch` batches of `batch_rows`).
    /// A statement without a result set gives an empty reader.
    pub fn into_reader(
        mut self,
        batch_rows: usize,
        prefetch: usize,
    ) -> Result<StatementReader, Error> {
        self.detached = true;
        let Some((columns, generation, execution)) = statement::result_columns(&self.stmt) else {
            return Ok(StatementReader::empty(self.conn.clone()));
        };
        let plan = match self.plan.take() {
            Some((plan, g)) if g == generation => plan,
            _ => Plan::new(&columns).map_err(|e| Error::driver("HY000", e))?,
        };
        let core = StreamCore::start(
            self.stmt.clone(),
            plan,
            generation,
            execution,
            batch_rows.max(1),
            prefetch.max(1),
            None,
        );
        let reader = core
            .reader()
            .map_err(|_| Error::driver("HY010", "the result is already being read"))?;
        Ok(StatementReader {
            schema: core.schema().clone(),
            reader: Some(reader),
            core: Some(core),
            _conn: self.conn.clone(),
        })
    }
}

impl fmt::Debug for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Statement")
            .field("id", &self.stmt.id)
            .finish_non_exhaustive()
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        if !self.detached {
            statement::close(&self.stmt, true);
        }
    }
}

/// Record batches of one result (`Statement::into_reader`). `Send`, so the
/// batches can be consumed on another thread; dropping it early discards the
/// unread rows and frees the connection.
pub struct StatementReader {
    schema: SchemaRef,
    reader: Option<StreamReader>,
    core: Option<Arc<StreamCore>>,
    _conn: Arc<ConnInner>,
}

impl StatementReader {
    fn empty(conn: Arc<ConnInner>) -> StatementReader {
        // An empty stream: no producer, no batches.
        StatementReader {
            schema: Arc::new(arrow_schema::Schema::empty()),
            reader: None,
            core: None,
            _conn: conn,
        }
    }

    /// Stops the read from another thread: the running fetch is cancelled,
    /// unread rows are discarded and the reader ends.
    pub fn cancel_handle(&self) -> CancelHandle {
        let core = self.core.as_ref().map(Arc::downgrade);
        CancelHandle(Arc::new(move || {
            if let Some(core) = core.as_ref().and_then(Weak::upgrade) {
                core.shutdown();
            }
        }))
    }

    /// This reader as an Arrow C stream, for a consumer in another language
    /// or shared library; releasing the stream frees the connection.
    pub fn into_ffi_stream(self) -> arrow_array::ffi_stream::FFI_ArrowArrayStream {
        arrow_array::ffi_stream::FFI_ArrowArrayStream::new(Box::new(self))
    }
}

impl Iterator for StatementReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.as_mut()?.next()
    }
}

impl RecordBatchReader for StatementReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

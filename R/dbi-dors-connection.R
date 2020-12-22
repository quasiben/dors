library(reticulate)

#' @include package.R
NULL

#' @include dbi-dors-driver.R
NULL

python_path <- system.file("python", package = "DORS")
pytools <- import_from_path("pytools", path = python_path)

#' Create Dask SQL or BlazingSQL Connection
#'
#' @param CTX character (dask or blazing)
#' @return \code{DORSConnection}
DORSConnection <- function(CTX = "dask") {
  if (CTX == "dask") {
    con <- pytools$dask_sql$create_context()
  }
  else if (CTX == "distributed") {
    con <- pytools$dask_sql$create_context_distributed()
  }
  else {
    con <- pytools$create_blazing_context()
  }
  structure(
    con,
    class = c("DORSConnection", class(con), "DBIConnection")
  )
}

#' @rdname DBI
#' @export
setClass(
  "DORSConnection",
  contains = "DBIConnection",
  slots = list()
)

#' @rdname DBI
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "DORSConnection",
  function(object) {
    cat("<DORSConnection>\n")
    # TODO: Print more details
  })

#' @rdname DBI
#' @inheritParams DBI::dbIsValid
#' @export
setMethod(
  "dbIsValid", "DORSConnection",
  function(dbObj, ...) {
    if (dbObj$dors_client == FALSE) {
      TRUE
    }
    tryCatch({
      dbGetQuery(dbObj, "select 1")
      TRUE
    }, error = function(e) {
      print(e)
      FALSE
    })
  })

#' #' @rdname DBI
#' #' @inheritParams DBI::dbDisconnect
#' #' @export
setMethod(
  "dbDisconnect", "DORSConnection",
  function(conn, ...) {
    if (!dbIsValid(conn)) {
      warning("Connection already closed.", call. = FALSE)
    }
    if (conn$dors_client != FALSE) {
      conn$dors_client$close()
    }
    TRUE
  })

#' @rdname DBI
#' @inheritParams DBI::dbSendQuery
#' @export
setMethod(
  "dbSendQuery", c("DORSConnection", "character"),
  function(conn, statement, ...) {
    DORSResult(connection = conn, statement = statement)
  })

#' @rdname DBI
#' @inheritParams DBI::dbSendStatement
#' @export
setMethod(
  "dbSendStatement", c("DORSConnection", "character"),
  function(conn, statement, ...) {
    DORSResult(connection = conn, statement = statement)
  })

#' @rdname DBI
#' @inheritParams DBI::dbDataType
#' @export
setMethod(
  "dbDataType", "DORSConnection",
  function(dbObj, obj, ...) {
    tryCatch(
      getMethod("dbDataType", "DBIObject", asNamespace("DBI"))(dbObj, obj, ...),
      error = function(e) testthat::skip("Not yet implemented: dbDataType(Connection)"))
  })

#' @rdname DBI
#' @inheritParams DBI::dbQuoteString
#' @export
setMethod(
  "dbQuoteString", c("DORSConnection", "character"),
  function(conn, x, ...) {
    # Optional
    getMethod("dbQuoteString", c("DBIConnection", "character"), asNamespace("DBI"))(conn, x, ...)
  })

#' @rdname DBI
#' @inheritParams DBI::dbQuoteIdentifier
#' @export
setMethod(
  "dbQuoteIdentifier", c("DORSConnection", "character"),
  function(conn, x, ...) {
    # Optional
    getMethod("dbQuoteIdentifier", c("DBIConnection", "character"), asNamespace("DBI"))(conn, x, ...)
  })

#' @rdname DBI
#' @inheritParams DBI::dbWriteTable
#' @param overwrite Allow overwriting the destination table. Cannot be
#'   `TRUE` if `append` is also `TRUE`.
#' @param append Allow appending to the destination table. Cannot be
#'   `TRUE` if `overwrite` is also `TRUE`.
#' @export
setMethod(
  "dbWriteTable", c("DORSConnection", "character", "data.frame"),
  function(conn, name, value, overwrite = FALSE, append = FALSE, ...) {
    df <- reticulate::r_to_py(value)
    pytools$dask_sql$create_table(conn, name, df)
  })

#' @rdname DBI
#' @inheritParams DBI::dbWriteTable
#' @param overwrite Allow overwriting the destination table. Cannot be
#'   `TRUE` if `append` is also `TRUE`.
#' @param append Allow appending to the destination table. Cannot be
#'   `TRUE` if `overwrite` is also `TRUE`.
#' @export
setMethod(
  "dbWriteTable", c("DORSConnection", "character", "character"),
  function(conn, name, value, overwrite = FALSE, append = FALSE, ...) {
    pytools$dask_sql$create_table(conn, name, value)
  })

#' @rdname DBI
#' @inheritParams DBI::dbReadTable
#' @export
setMethod(
  "dbReadTable", c("DORSConnection", "character"),
  function(conn, name, ...) {
    testthat::skip("Not yet implemented: dbReadTable(Connection, character)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbListTables
#' @export
setMethod(
  "dbListTables", "DORSConnection",
  function(conn, ...) {
    testthat::skip("Not yet implemented: dbListTables(Connection)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbExistsTable
#' @export
setMethod(
  "dbExistsTable", c("DORSConnection", "character"),
  function(conn, name, ...) {
    testthat::skip("Not yet implemented: dbExistsTable(Connection)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbListFields
#' @export
setMethod(
  "dbListFields", c("DORSConnection", "character"),
  function(conn, name, ...) {
    testthat::skip("Not yet implemented: dbListFields(Connection, character)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbRemoveTable
#' @export
setMethod(
  "dbRemoveTable", c("DORSConnection", "character"),
  function(conn, name, ...) {
    testthat::skip("Not yet implemented: dbRemoveTable(Connection, character)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "DORSConnection",
  function(dbObj, ...) {
    testthat::skip("Not yet implemented: dbGetInfo(Connection)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbBegin
#' @export
setMethod(
  "dbBegin", "DORSConnection",
  function(conn, ...) {
    testthat::skip("Not yet implemented: dbBegin(Connection)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbCommit
#' @export
setMethod(
  "dbCommit", "DORSConnection",
  function(conn, ...) {
    testthat::skip("Not yet implemented: dbCommit(Connection)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbRollback
#' @export
setMethod(
  "dbRollback", "DORSConnection",
  function(conn, ...) {
    testthat::skip("Not yet implemented: dbRollback(Connection)")
  })

#' @export
DBI::dbIsReadOnly

#' @export
DBI::dbQuoteLiteral

#' @export
DBI::dbUnquoteIdentifier

#' @export
DBI::dbGetQuery

#' @export
DBI::dbExecute

#' @export
DBI::dbCreateTable

#' @export
DBI::dbAppendTable

#' @export
DBI::dbListObjects

#' @export
DBI::dbWithTransaction


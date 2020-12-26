#' @include dbi-dors-result.R
NULL

#' Run SQL through Dask/RAPIDS/Blazing Engine
#'
#' @param res object res object containing sql statement and db connection
#'
#' @export
run_sql <- function(res) {
  python_path <- system.file("python", package = "DORS")
  pytools <- reticulate::import_from_path("pytools", path = python_path, convert = FALSE)
  df <- pytools$dask_tools$run_sql(res@connection, res@statement)
  df
}

DORSResult <- function(connection, statement) {
  # TODO: Initialize result
  new("DORSResult", connection = connection, statement = statement)
}

#' @rdname DBI
#' @export
setClass(
  "DORSResult",
  contains = "DBIResult",
  slots = list(
    connection = "DORSConnection",
    statement = "character"
  )
)

#' @rdname DBI
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "DORSResult",
  function(object) {
    cat("<DORSResult>\n")
    # TODO: Print more details
  })

#' @rdname DBI
#' @inheritParams DBI::dbClearResult
#' @export
setMethod(
  "dbClearResult", "DORSResult",
  function(res, ...) {
    TRUE
  })

#' @rdname DBI
#' @inheritParams DBI::dbFetch
#' @export
setMethod(
  "dbFetch", "DORSResult",
  function(res, n = -1, ...) {
    dfFetch <- run_sql(res)
    if (hasName(dfFetch, "to_arrow")) {
      dfFetch <- dfFetch$to_arrow()
    }
    
    else if (inherits(dfFetch, "python.builtin.object")) {
      dfFetch <- reticulate::py_to_r(dfFetch)
    }
    dfFetch
  })

#' @rdname DBI
#' @inheritParams DBI::dbHasCompleted
#' @export
setMethod(
  "dbHasCompleted", "DORSResult",
  function(res, ...) {
    testthat::skip("Not yet implemented: dbHasCompleted(Result)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "DORSResult",
  function(dbObj, ...) {
    # Optional
    getMethod("dbGetInfo", "DBIResult", asNamespace("DBI"))(dbObj, ...)
  })

#' @rdname DBI
#' @inheritParams DBI::dbIsValid
#' @export
setMethod(
  "dbIsValid", "DORSResult",
  function(dbObj, ...) {
    testthat::skip("Not yet implemented: dbIsValid(Result)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetStatement
#' @export
setMethod(
  "dbGetStatement", "DORSResult",
  function(res, ...) {
    testthat::skip("Not yet implemented: dbGetStatement(Result)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbColumnInfo
#' @export
setMethod(
  "dbColumnInfo", "DORSResult",
  function(res, ...) {
    testthat::skip("Not yet implemented: dbColumnInfo(Result)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetRowCount
#' @export
setMethod(
  "dbGetRowCount", "DORSResult",
  function(res, ...) {
    testthat::skip("Not yet implemented: dbGetRowCount(Result)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetRowsAffected
#' @export
setMethod(
  "dbGetRowsAffected", "DORSResult",
  function(res, ...) {
    testthat::skip("Not yet implemented: dbGetRowsAffected(Result)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbBind
#' @export
setMethod(
  "dbBind", "DORSResult",
  function(res, params, ...) {
    testthat::skip("Not yet implemented: dbBind(Result)")
  })

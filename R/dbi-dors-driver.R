library(reticulate)
library(DBI)

#' @include package.R
NULL

#' DBI methods
#'
#' Implementations of pure virtual functions defined in the `DBI` package.
#' @name DBI
NULL

#' DORS driver
#'
#' TBD.
#'
#' @export
#' @import methods DBI
#' @examples
#' \dontrun{
#' #' library(DBI)
#' DORS::DORS()
#' }
DORS <- function() {
  new("DORSDriver")
}

#' @rdname DBI
#' @export
setClass("DORSDriver", contains = "DBIDriver")

#' @rdname DBI
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "DORSDriver",
  function(object) {
    cat("<DORSDriver>\n")
    # TODO: Print more details
  })

#' @rdname DBI
#' @inheritParams DBI::dbConnect
#' @param CTX character (dask or blazing)
#' @export
setMethod(
  "dbConnect", "DORSDriver",
  function(drv, CTX = "dask", SCHEDULER = FALSE) {
    # TODO: Remove skip() call
    DORSConnection(CTX, SCHEDULER)
  }
)

#' @rdname DBI
#' @inheritParams DBI::dbDataType
#' @export
setMethod(
  "dbDataType", "DORSDriver",
  function(dbObj, obj, ...) {
    # Optional: Can remove this if all data types conform to SQL-92
    tryCatch(
      getMethod("dbDataType", "DBIObject", asNamespace("DBI"))(dbObj, obj, ...),
      error = function(e) testthat::skip("Not yet implemented: dbDataType(Driver)"))
  })

#' @rdname DBI
#' @inheritParams DBI::dbDataType
#' @export
setMethod(
  "dbDataType", c("DORSDriver", "list"),
  function(dbObj, obj, ...) {
    # rstats-db/DBI#70
    testthat::skip("Not yet implemented: dbDataType(Driver, list)")
  })

#' #' @rdname DBI
#' #' @inheritParams DBI::dbIsValid
#' #' @export
#' setMethod(
#'   "dbIsValid", "DORSDriver",
#'   function(dbObj, ...) {
#'     testthat::skip("Not yet implemented: dbIsValid(Driver)")
#'   })

#' @rdname DBI
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "DORSDriver",
  function(dbObj, ...) {
    testthat::skip("Not yet implemented: dbGetInfo(Driver)")
  })

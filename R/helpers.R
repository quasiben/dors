#' Helper Function to Load Random Timeseries
#'
#' @param CTX object DB context
#' @param name character name of the table (default: ts)
#'
#' @export
create_timeseries <- function(CTX, name = 'ts') {
  python_path <- system.file("python", package = "DORS")
  pytools <- reticulate::import_from_path("pytools", path = python_path)
  pytools$dask_tools$create_timeseries(CTX,  name)
}
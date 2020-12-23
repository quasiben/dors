skip_if_no_dask_sql <- function() {
  if (!reticulate::py_module_available("dask_sql")) {
    skip("Dask SQL not available for testing...")
  }
}
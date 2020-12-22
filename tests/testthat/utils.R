skip_if_no_dask_sql <- function() {
  print("FOOBAR")
  if (!reticulate::py_module_available("dask_sql"))
    print("SKIPPING")
    skip("Dask SQL not available for testing")
}
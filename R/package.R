#' Dask or Rapids SQL for R
NULL

# globals
.globals <- new.env(parent = emptyenv())
.globals$dask_sql <- NULL


.onLoad <- function(libname, pkgname) {
  CONDA_PREFIX <- Sys.getenv("CONDA_PREFIX", unset = NA)
  if (!(is.na(CONDA_PREFIX))) {
    
    CONDA_ENV <- sapply(strsplit(CONDA_PREFIX, "/"), tail, 1)
    reticulate::use_condaenv(CONDA_ENV)
    dors_python <- paste(CONDA_PREFIX, "/bin/python", sep = "")
    Sys.setenv(RETICULATE_PYTHON = dors_python)
    
    .globals$dask_sql <<- reticulate::import("dask_sql", delay_load = TRUE)
    
  }
  
  java_home <- Sys.getenv("JAVA_HOME", unset = NA)
  if (is.na(java_home)) {
   if (!(is.na(CONDA_PREFIX))) {
     Sys.setenv(JAVA_HOME = CONDA_PREFIX)
     os <- import("os")
     reticulate::py_set_item(os$environ, "JAVA_HOME", CONDA_PREFIX)
     }
   }


}

.onAttach <- function(libname, pkgname) {
}

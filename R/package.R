#' Dask or Rapids SQL for R

# Main DORS module
DORS <- NULL

# globals
.globals <- new.env(parent = emptyenv())
.globals$dask_sql <- NULL


.onLoad <- function(libname, pkgname) {
  CONDA_PREFIX <- Sys.getenv("CONDA_PREFIX", unset = NA)
  if (!(is.na(CONDA_PREFIX))) {
    
    CONDA_ENV <- dplyr::last(strsplit(CONDA_PREFIX, '/')[[1]])
    reticulate::use_condaenv(CONDA_ENV)
    dors_python <- paste(CONDA_PREFIX, "/bin/python", sep = "")
    Sys.setenv(RETICULATE_PYTHON = dors_python)
    
    .globals$dask_sql <<- reticulate::import("dask_sql", delay_load = TRUE)
    
  }
  
  java_home <- Sys.getenv("JAVA_HOME", unset = NA)
  if (is.na(java_home)) {
   if (!(is.na(CONDA_PREFIX))) {
     Sys.setenv(JAVA_HOME = CONDA_PREFIX)
     os <- reticulate::import("os")
     reticulate::py_set_item(os$environ, "JAVA_HOME", CONDA_PREFIX)
     }
   }


}

.onAttach <- function(libname, pkgname) {
}

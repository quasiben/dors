#' @export
install_dors <- function(envname = "r-reticulate") {
  
  conda_install(c("dask", "distributed", "dask-sql", "python==3.8"), 
              envname = envname, 
              method = "conda", 
              channel = "conda-forge"
  )
}
source("utils.R")

test_that("select all", {
  skip_if_no_dask_sql()

  df = data.frame(
    ID = c("b","b","b","a","a","c"),
    a = 1:6,
    b = 7:12,
    c = 13:18
  )
  db <- dbConnect(DORS::DORS(), CTX = 'distributed')
  dbWriteTable(db, "example", df)
  res <- dbGetQuery(db, "SELECT * FROM example")
  dbDisconnect(db)
  expect_true(is.null(py_to_r(db$dors_client$scheduler)))
})
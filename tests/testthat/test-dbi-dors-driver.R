source("utils.R")

test_that("select all", {
  skip_if_no_dask_sql()
  df = data.frame(
    ID = c("b","b","b","a","a","c"),
    a = 1:6,
    b = 7:12,
    c = 13:18
  )
  db <- dbConnect(DORS::DORS(), CTX = 'dask')
  dbWriteTable(db, "example", df)
  res <- dbGetQuery(db, "SELECT * FROM example")

  expect_equal(res, df, ignore_attr = TRUE)

})

test_that("select column and filter", {
  skip_if_no_dask_sql()
  df = data.frame(
    ID = c("b","b","b","a","a","c"),
    a = 1:6,
    b = 7:12,
    c = 13:18
  )
  db <- dbConnect(DORS::DORS(), CTX = 'dask')
  dbWriteTable(db, "example", df)
  res <- dbGetQuery(db, "SELECT b FROM example where c > 15")
  df <- df[df$c > 15,]["b"]
  expect_equal(res, df, ignore_attr = TRUE)

})

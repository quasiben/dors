source('utils.R')

test_that("create timeseries works", {
  skip_if_no_dask_sql()
  
  db <- dbConnect(DORS::DORS(), CTX = 'dask')
  create_timeseries(db)
  create_timeseries(db, 'foobar')
  expect_equal(names(db$tables), c("foobar", "ts"))
  
})

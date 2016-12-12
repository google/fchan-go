# `fchan`: Fast Channels in Go

This package contains implementations of fast and scalable channels in Go.
Implementation is in `src/fchan`. To run benchmarks, run `src/bench/bench.go`.
`bench.go` is very rudimentary, and modifying the source may be necessary
depending on what you want to run; that will change in the future.  For details
on the algorithm, check out the writeup directory, it includes a pdf and the
pandoc markdown used to generate it. 

**This is a proof of concept only**. This code should *not* be run in
production.  Comments, criticisms and bugs are all welcome!

## Disclaimer

This is not an official Google product.

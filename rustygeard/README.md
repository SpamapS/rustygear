# Rustygeard

Rustygeard is a Rust-Native implementation of a [Gearman](https://gearman.org/) server.

## Features
- Admin protocol (Partial)
- Coalescing of unique foreground jobs
- Round-robin dequeue on multi-function workers (--round-robin on C/C++ gearmand)
- Experimental TLS using native [rustls](https://crates.io/crates/rustls)

Rustygeard is still very much Alpha quality software lacking detailed
documentation and many of the features of the more mature C/C++ gearmand.


## TLS

Please note that this TLS support is *experimental*. It has not been used in production and is meant for experimentation and testing purposes only.

To enable TLS features, pass `--tls` to rustygeard. This will also require `--tls-key` and `--tls-cert`. These should be passed the path to a PEM encoded private key and PEM encoded full cert chain, respectively.

If you want clients to use TLS client auth, pass --tls-ca, which will make rustygeard request client certificates and validate that they are signed by said CA.
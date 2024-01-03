I saw this [Blog Post](https://www.morling.dev/blog/one-billion-row-challenge/) on a Billion Row challenge for Java so naturally I tried implementing a solution in Python & Rust using mainly polars. 

To run my code you will need to have Rust & Python installed, or you can run it in a Dev Container.

Thanks to [Coriolinus](https://github.com/coriolinus/1brc) for the full implementation in Rust.


## Generating the data file

There is a feature-gated binary which can create the appropriate measurements list, as follows:

```sh
time cargo run --release --features generator --bin generate 1000000000
```

 
## Run the challenge

```sh
cargo build --release && time target/release/1brc >/dev/null
```

## Results

Running the code on my laptop, which is equipped with an i7-1185G7 @ 3.00GHz and 16GB of RAM.

| Implementation | Time | 
|----------------|------|
|        Python        |  - Crashed   |
|        Python + Pandas        |   - Crashed   |
|        Python + Polars        |   61.41 sec   |
|        Rust + Polars        |   39 sec   |  
|       Rust STD Libray        |   17 sec  |



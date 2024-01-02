I saw this [Blog Post](https://www.morling.dev/blog/one-billion-row-challenge/) on a Billion Row challenge for Java so naturally I tried implementing a solution in Python & Rust using mainly polars.  

You'll need to download the Java code from the blog post and run it to generate the data. You'll need to follow the README instructions (here)[https://github.com/gunnarmorling/1brc] to generate the data.


To run my code you will need to have Rust & Python installed, or you can run it in a Dev Container.


Running the code on my laptop, which is equipped with an i7-1185G7 @ 3.00GHz and 32GB of RAM, I achieved the following runtimes within a Dev Container. 

| Implementation | Time | 
|----------------|------|
|        Python        |  - Crashed   |
|        Python + Pandas        |   - Crashed   |
|        Python + Polars        |   61.41 sec   |
|        Rust + Polars        |   39s   |  
|       Rust STD Libray        |   149.69s  |



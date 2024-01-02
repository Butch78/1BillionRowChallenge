I saw this [Blog Post](https://www.morling.dev/blog/one-billion-row-challenge/) on a Billion Row challenge for Java so naturally I tried implementing a solution in Python & Rust using mainly polars.  

You'll need to download the Java code from the blog post and run it to generate the data.  I used the following command to generate the data. 


Running the code on my laptop, which is equipped with an i7-1185G7 @ 3.00GHz and 32GB of RAM, I achieved the following runtimes within a Dev Container. 

| Implementation | Time | 
|----------------|------|
|        Python        |   33.76s   |
|        Python + Pandas        |   39.21s   |
|        Python + Polars        |   4.94 s   |
|        Rust + Polars        |   4.5s   |  
|       Rust STD Libray        |   18.5s  |



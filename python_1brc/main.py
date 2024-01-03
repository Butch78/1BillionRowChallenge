import os
from multiprocessing import Pool
import polars as pl
import pandas as pd


# Created by Koen Vossen, 
# Github: https://github.com/koenvo
# Twitter/x Handle: https://twitter.com/mr_le_fox
# https://x.com/mr_le_fox/status/1741893400947839362?s=20
def create_polars_df():
    pl.Config.set_streaming_chunk_size(4000000)
    return (
        
        pl.scan_csv("measurements.txt", separator=";", has_header=False, new_columns=["station", "measure"])
        .group_by(by="station")
        .agg(
            max = pl.col("measure").max(),
            min = pl.col("measure").min(),
            mean = pl.col("measure").mean()
        )
        .sort("station")
        .collect(streaming=True)
    )


def create_pandas_df():
    
    df = pd.read_csv("measurements.txt", sep=";", header=None, names=["station", "measure"])
    df = df.groupby("station").agg(["min", "max", "mean"])
    df.columns = df.columns.droplevel()
    df = df.sort_index()
    return df


# Created by Koen Vossen, 
# Github: https://github.com/koenvo
# Twitter/x Handle: https://twitter.com/mr_le_fox
# Source: https://gist.github.com/koenvo/81e795ff2e0861e75e6dac1630171ce6


# Notes:
# a) Let every process handle a single chunk.
# b) Use as many processes as cores
CHUNK_COUNT = 8
CONCURRENCY = 8


def read_chunk(filename, chunk_start, chunk_size):
    station_measurements = {}

    with open(filename, "r") as fp:
        fp.seek(chunk_start)
        bytes_read = 0

        while bytes_read < chunk_size:
            for line in fp:
                bytes_read += len(line) + 1
                if bytes_read > chunk_size:
                    break

                tmp = line.split(";")

                station = tmp[0]
                measurement = float(tmp[1])

                try:
                    item = station_measurements[station]
                    item[0] = min(item[0], measurement)
                    item[1] = max(item[1], measurement)
                    item[2] += measurement
                    item[3] += 1
                except KeyError:
                    station_measurements[station] = [measurement, measurement, measurement, 1]

    return station_measurements

# Created by Koen Vossen, 
# Github: https://github.com/koenvo
# Twitter/x Handle: https://twitter.com/mr_le_fox
def create_df(filename):
    size = os.path.getsize(filename)

    chunk_size = size // CHUNK_COUNT

    start_positions = [
        i * chunk_size
        for i in range(CHUNK_COUNT)
    ]

    # Step 1: adjust chunks to snap to newlines
    with open(filename, "r") as fp:
        for i, start in enumerate(start_positions):
            fp.seek(start)
            data = fp.read(1024)
            pos = data.index("\n")

            # don't change first position
            if i > 0:
                # start just after newline
                start_positions[i] += pos + 1

    # Step 2: define chunks start and size
    chunks = []
    for start, end in zip(start_positions, start_positions[1:] + [size]):
        chunks.append((filename, start, end - start))

    with Pool(CONCURRENCY) as pool:
        res = pool.starmap(
            read_chunk,
            chunks
        )

    station_measurements = {}
    for chunk in res:
        for station, (min_, max_, sum_, count) in chunk.items():
            try:
                item = station_measurements[station]
                item[0] = min(item[0], min_)
                item[1] = max(item[1], max_)
                item[2] += sum_
                item[3] += count
            except KeyError:
                station_measurements[station] = [min_, max_, sum_, count]

    return [
        (station, min_, max_, sum_ / count) for
        (station, (min_, max_, sum_, count)) in list(sorted(station_measurements.items()))
    ]


if __name__ == "__main__":
    import time
    start_time = time.time()
    df = create_df("measurements.txt")
    took = time.time() - start_time
    print(f"Took: {took:.2f} sec")
    print(df)

    start_time = time.time()
    df = create_polars_df()
    took = time.time() - start_time
    print(df)
    print(df.head())
    print(f"Polars Took: {took:.2f} sec")

    # start_time = time.time()
    # df = create_pandas_df()
    # took = time.time() - start_time
    # print(df)
    # print(df.head())
    # print(f"Pandas Took: {took:.2f} sec")


# Spark weighted-overlay shuffle benchmark

On a 2,048 × 2,048 tiled raster at H3 resolution 11, the native classifier finalized 732,922 of 736,392 output cells inside their scan partitions. The other cells passed through the partial-state reducer. Both merge paths matched a serial weighted overlay on every cell ID, weighted count, sum and mean.

| Implementation | Spark shuffle writes (MiB) | Median wall time (s) |
| --- | ---: | ---: |
| Original merge, all rows carry partial state | 50.88 | 1.959 |
| Worker finalization, ordinary DataFrame merge | 20.65 | 1.679 |
| Worker finalization, caller-persisted input and boundary-only exchange | 0.40 | 1.602 |

These are seven timed runs per path after warm-up, using 256 × 256 tiles, 16 chunks per scan partition and four reducer tasks. The cached timing includes populating and releasing the cache for each action. All shuffle reads were local. These results measure single-host Spark execution and leave cross-host network savings unmeasured.

## Six partitioning scenarios

The 1,024 × 1,024 fixture has 64 TIFF tiles of 128 × 128 pixels. An independent pass outside the timed runs counted how many scan partitions actually contributed to each cell. Single-owner state bytes are the raw encoded payload for those cells. Spark write bytes come from the task event log and include the exchanged rows with Spark's serialization and compression. The final two columns compare the original merge with worker finalization using uncached input. Times are medians of three actions per path.

| H3 resolution | Target chunks | Scan partitions | Single-owner cells / all cells | Single-owner state (MiB) | Share of state bytes | Shuffle MiB, before → after | Merge seconds, before → after |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 9 | 1 | 64 | 2,925 / 3,899 | 0.232 | 58.6% | 0.391 → 0.300 | 0.471 → 0.471 |
| 9 | 4 | 16 | 3,465 / 3,899 | 0.274 | 79.6% | 0.318 → 0.193 | 0.153 → 0.161 |
| 9 | 16 | 4 | 3,749 / 3,899 | 0.297 | 92.5% | 0.288 → 0.140 | 0.086 → 0.087 |
| 11 | 1 | 64 | 177,606 / 184,763 | 14.058 | 92.5% | 13.368 → 6.334 | 0.946 → 0.860 |
| 11 | 4 | 16 | 181,676 / 184,763 | 14.381 | 96.7% | 13.042 → 5.677 | 0.640 → 0.580 |
| 11 | 16 | 4 | 183,733 / 184,763 | 14.543 | 98.9% | 12.883 → 5.344 | 0.531 → 0.488 |

With four scan partitions at resolution 11, an identity Arrow pass took 0.454 s directly and 0.529 s with a four-way exchange. With 64 scan partitions the exchange control was faster, as it consolidated the downstream Arrow work into four tasks. The control measures both data movement and the change in task layout.

Before implementing native classification, an experiment used the exact owner IDs from the independent pass. On the smaller four-partition fixture, the boundary-only exchange took 0.539 s versus 0.548 s for the production baseline. On the larger fixture it took 1.693 s versus 1.959 s. The production classifier makes its decision from chunk bounds on each worker and requires one raster scan.

## Reproduce

Use Spark 4.1.4, Java 21, PyArrow, NumPy and the release Python native library. The harness uses Spark local mode with two task slots, four shuffle partitions, adaptive execution disabled, and the same synthetic WGS84 terrain generator as the Sedona demo. Each action writes to Spark's no-op sink to consume its rows. Run order is shuffled with a fixed seed after the plans are built, with one warm-up per mode.

```sh
python examples/sedona_spark/cluster/benchmark_overlay_shuffle.py
python examples/sedona_spark/cluster/benchmark_overlay_shuffle.py --probe --side 2048 --tile 256 --repeats 7
python examples/sedona_spark/cluster/benchmark_overlay_shuffle.py --native --side 2048 --tile 256 --repeats 7
```

To reproduce the original baseline, point `DGGS_STREAM_PYTHON_LIB` at the release library built from `dd413a9` and run the first two commands with the matching Python adapter. For the optimized run, use the updated library and adapter. JSON output includes every wall time, single-owner payload bytes, and median shuffle bytes written, locally read and remotely read.

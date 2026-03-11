# EdgeWARN-Core Performance Improvements Implementation Plan

## 1. Redundant File I/O (Cell History Cache)
**The Bottleneck:**
Currently, modules like `StormCast` and `MorphoWind` independently read from the file system (`fs.CELL_DIR / f"{cell_id}.json"`) for every cell during each integration tick. This causes duplicate file I/O operations for the same data, slowing down the processing cycle.

**The Implementation:**
To optimize this, we will centralize the history loading.
1. Create a `CellHistoryCache` class in `src/EdgeWARN/core/ctam/util/history.py`. This class will store parsed history lists in a dictionary keyed by `cell_id`.
2. In the main `run_ctam()` loop, instantiate this cache and pass it down.
3. When a module needs history, it requests it from the cache. The cache performs the file read only once per `cell_id` and serves from memory thereafter.

## 2. Sequential Dataset Integration (Parallel Integration)
**The Bottleneck:**
`main.py` processes grouped MRMS/RAP datasets sequentially. If multiple non-dependent datasets exist (e.g., AzShear, EchoTops), they are calculated one after the other, extending the total execution time significantly (e.g., 30s for 15 datasets).

**The Implementation:**
Now that lazy loading is implemented (preventing memory exhaustion), we can parallelize dataset processing.
1. Use `concurrent.futures.ProcessPoolExecutor` to distribute the dataset groups across available CPU cores.
2. Refactor the `result_cells` update logic. Because processes don't share memory, workers will return partial updates (or specific calculated properties) which the main thread will then reduce/merge back into the primary `result_cells` list.

## 3. API Index Full Directory Scans
**The Bottleneck:**
`index_manager.py` rebuilds the active cell index by scanning the entire `EdgeWARN_input/cells` directory using `glob()` on every update. With thousands of files, this creates noticeable disk latency.

**The Implementation:**
We will transition to an incremental indexing strategy.
1. Modify `_initialize_cell_index()` into an `IncrementalCellIndex`.
2. The index will be loaded into a dictionary (`{cell_id: lastUpdated_timestamp}`) upon startup.
3. When `update_cell_index(cell_ids)` is called, it will simply update the timestamps for the provided `cell_ids` in the dictionary and save the updated index to disk, avoiding any directory scanning.
4. Old cells will be pruned programmatically based on the timestamp dictionary.

from __future__ import annotations

import json
import os
import random
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from tabulate import tabulate
from tqdm import tqdm

from db import FACULTIES, get_db

# ── Config ──
NUM_OPS = 5_000          # operations per test
CONCURRENT_THREADS = [1, 2, 4, 8, 16]
RESULTS_DIR = "results"

os.makedirs(RESULTS_DIR, exist_ok=True)

db = get_db()


def uid() -> str:
    return uuid.uuid4().hex[:12]


def preload_ids(n: int = 10_000) -> list[str]:
    pipeline = [{"$sample": {"size": n}}, {"$project": {"student_id": 1, "_id": 0}}]
    return [doc["student_id"] for doc in db.students.aggregate(pipeline)]


def test_bulk_insert(n: int) -> dict:
    """Bulk inserts of n student documents."""
    docs = []
    for _ in range(n):
        docs.append({
            "student_id": uid(),
            "last_name": f"Test_{uid()[:6]}",
            "first_name": f"User_{uid()[:4]}",
            "email": f"{uid()}@test.com",
            "phone": "+70000000000",
            "faculty": random.choice(FACULTIES),
            "program": "Бакалавриат",
            "year": random.randint(1, 4),
            "enrollment_date": datetime.utcnow(),
            "is_active": True,
            "created_at": datetime.utcnow(),
        })

    latencies = []
    batch_size = 500
    start = time.perf_counter()
    for i in range(0, n, batch_size):
        batch = docs[i:i + batch_size]
        t0 = time.perf_counter()
        db.students.insert_many(batch, ordered=False)
        latencies.append((time.perf_counter() - t0) * 1000)
    total_time = time.perf_counter() - start

    # Clean up test data
    db.students.delete_many({"last_name": {"$regex": "^Test_"}})

    return {
        "name": "Bulk INSERT",
        "ops": n,
        "total_sec": total_time,
        "ops_per_sec": n / total_time,
        "avg_latency_ms": statistics.mean(latencies),
        "p50_ms": statistics.median(latencies),
        "p95_ms": np.percentile(latencies, 95),
        "p99_ms": np.percentile(latencies, 99),
        "latencies": latencies,
    }


def test_read_by_shard_key(ids: list[str], n: int) -> dict:
    """Targeted reads by shard key (student_id) — should hit single shard."""
    latencies = []
    for _ in tqdm(range(n), desc="Read by shard key", leave=False):
        sid = random.choice(ids)
        t0 = time.perf_counter()
        db.students.find_one({"student_id": sid})
        latencies.append((time.perf_counter() - t0) * 1000)

    total_time = sum(latencies) / 1000
    return {
        "name": "READ (shard key)",
        "ops": n,
        "total_sec": total_time,
        "ops_per_sec": n / total_time,
        "avg_latency_ms": statistics.mean(latencies),
        "p50_ms": statistics.median(latencies),
        "p95_ms": np.percentile(latencies, 95),
        "p99_ms": np.percentile(latencies, 99),
        "latencies": latencies,
    }


def test_read_scatter_gather(n: int) -> dict:
    """Reads by non-shard field (faculty) — triggers scatter-gather."""
    latencies = []
    for _ in tqdm(range(n), desc="Read scatter-gather", leave=False):
        fac = random.choice(FACULTIES)
        t0 = time.perf_counter()
        list(db.students.find({"faculty": fac}).limit(10))
        latencies.append((time.perf_counter() - t0) * 1000)

    total_time = sum(latencies) / 1000
    return {
        "name": "READ (scatter-gather)",
        "ops": n,
        "total_sec": total_time,
        "ops_per_sec": n / total_time,
        "avg_latency_ms": statistics.mean(latencies),
        "p50_ms": statistics.median(latencies),
        "p95_ms": np.percentile(latencies, 95),
        "p99_ms": np.percentile(latencies, 99),
        "latencies": latencies,
    }


def test_update_by_shard_key(ids: list[str], n: int) -> dict:
    """Updates by shard key (student_id)."""
    latencies = []
    for _ in tqdm(range(n), desc="Update by shard key", leave=False):
        sid = random.choice(ids)
        t0 = time.perf_counter()
        db.students.update_one(
            {"student_id": sid},
            {"$set": {"year": random.randint(1, 6)}}
        )
        latencies.append((time.perf_counter() - t0) * 1000)

    total_time = sum(latencies) / 1000
    return {
        "name": "UPDATE (shard key)",
        "ops": n,
        "total_sec": total_time,
        "ops_per_sec": n / total_time,
        "avg_latency_ms": statistics.mean(latencies),
        "p50_ms": statistics.median(latencies),
        "p95_ms": np.percentile(latencies, 95),
        "p99_ms": np.percentile(latencies, 99),
        "latencies": latencies,
    }


def test_aggregation(n: int) -> dict:
    """Run aggregation pipeline multiple times."""
    latencies = []
    for _ in tqdm(range(n), desc="Aggregation", leave=False):
        t0 = time.perf_counter()
        list(db.grades.aggregate([
            {"$group": {
                "_id": "$student_id",
                "avg": {"$avg": "$grade"},
                "cnt": {"$sum": 1}
            }},
            {"$match": {"cnt": {"$gte": 3}}},
            {"$sort": {"avg": -1}},
            {"$limit": 10}
        ], allowDiskUse=True))
        latencies.append((time.perf_counter() - t0) * 1000)

    total_time = sum(latencies) / 1000
    return {
        "name": "Aggregation pipeline",
        "ops": n,
        "total_sec": total_time,
        "ops_per_sec": n / total_time,
        "avg_latency_ms": statistics.mean(latencies),
        "p50_ms": statistics.median(latencies),
        "p95_ms": np.percentile(latencies, 95),
        "p99_ms": np.percentile(latencies, 99),
        "latencies": latencies,
    }


def test_mixed_workload(ids: list[str], n: int) -> dict:
    """Mixed workload: 70% read, 20% update, 10% insert."""
    latencies = {"read": [], "update": [], "insert": []}

    for _ in tqdm(range(n), desc="Mixed workload", leave=False):
        r = random.random()
        if r < 0.7:
            sid = random.choice(ids)
            t0 = time.perf_counter()
            db.students.find_one({"student_id": sid})
            latencies["read"].append((time.perf_counter() - t0) * 1000)
        elif r < 0.9:
            sid = random.choice(ids)
            t0 = time.perf_counter()
            db.students.update_one(
                {"student_id": sid},
                {"$set": {"year": random.randint(1, 6)}}
            )
            latencies["update"].append((time.perf_counter() - t0) * 1000)
        else:
            t0 = time.perf_counter()
            db.students.insert_one({
                "student_id": uid(),
                "last_name": "MixTest",
                "first_name": "User",
                "email": f"{uid()}@test.com",
                "phone": "+70000000000",
                "faculty": random.choice(FACULTIES),
                "program": "Бакалавриат",
                "year": 1,
                "enrollment_date": datetime.utcnow(),
                "is_active": True,
                "created_at": datetime.utcnow(),
            })
            latencies["insert"].append((time.perf_counter() - t0) * 1000)

    all_lat = latencies["read"] + latencies["update"] + latencies["insert"]
    total_time = sum(all_lat) / 1000
    db.students.delete_many({"last_name": "MixTest"})

    return {
        "name": "Mixed (70R/20U/10I)",
        "ops": n,
        "total_sec": total_time,
        "ops_per_sec": n / total_time,
        "avg_latency_ms": statistics.mean(all_lat),
        "p50_ms": statistics.median(all_lat),
        "p95_ms": np.percentile(all_lat, 95),
        "p99_ms": np.percentile(all_lat, 99),
        "latencies": all_lat,
        "breakdown": {
            "read_avg_ms": statistics.mean(latencies["read"]) if latencies["read"] else 0,
            "update_avg_ms": statistics.mean(latencies["update"]) if latencies["update"] else 0,
            "insert_avg_ms": statistics.mean(latencies["insert"]) if latencies["insert"] else 0,
        },
    }


def _worker_reads(ids, n):
    """Worker function for concurrent test."""
    local_db = get_db()
    latencies = []
    for _ in range(n):
        sid = random.choice(ids)
        t0 = time.perf_counter()
        local_db.students.find_one({"student_id": sid})
        latencies.append((time.perf_counter() - t0) * 1000)
    return latencies


def test_concurrent(ids: list[str], ops_total: int = 2000) -> list[dict]:
    """Run concurrent read tests with varying thread counts."""
    results = []
    for n_threads in CONCURRENT_THREADS:
        ops_per_thread = ops_total // n_threads
        all_latencies = []

        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=n_threads) as pool:
            futures = [pool.submit(_worker_reads, ids, ops_per_thread) for _ in range(n_threads)]
            for f in as_completed(futures):
                all_latencies.extend(f.result())
        elapsed = time.perf_counter() - start

        results.append({
            "threads": n_threads,
            "ops": len(all_latencies),
            "total_sec": elapsed,
            "ops_per_sec": len(all_latencies) / elapsed,
            "avg_latency_ms": statistics.mean(all_latencies),
            "p99_ms": np.percentile(all_latencies, 99),
        })
        print(f"  {n_threads} threads: {len(all_latencies)/elapsed:.0f} ops/s, "
              f"avg={statistics.mean(all_latencies):.2f}ms")

    return results


def plot_throughput(results: list[dict]):
    names = [r["name"] for r in results]
    ops = [r["ops_per_sec"] for r in results]

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(names)))
    bars = ax.barh(names, ops, color=colors, edgecolor="white", linewidth=0.5)
    ax.set_xlabel("Операций / сек", fontsize=12)
    ax.set_title("Пропускная способность по типам операций", fontsize=14, fontweight="bold")
    for bar, val in zip(bars, ops):
        ax.text(bar.get_width() + max(ops) * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{val:,.0f}", va="center", fontsize=10)
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{RESULTS_DIR}/throughput_chart.png", dpi=150)
    plt.close()
    print(f"  📊  Saved {RESULTS_DIR}/throughput_chart.png")


def plot_latency(results: list[dict]):
    names = [r["name"] for r in results]
    avg = [r["avg_latency_ms"] for r in results]
    p50 = [r["p50_ms"] for r in results]
    p95 = [r["p95_ms"] for r in results]
    p99 = [r["p99_ms"] for r in results]

    x = np.arange(len(names))
    width = 0.2

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.bar(x - 1.5 * width, avg, width, label="AVG", color="#2196F3")
    ax.bar(x - 0.5 * width, p50, width, label="P50", color="#4CAF50")
    ax.bar(x + 0.5 * width, p95, width, label="P95", color="#FF9800")
    ax.bar(x + 1.5 * width, p99, width, label="P99", color="#F44336")

    ax.set_ylabel("Латентность (мс)", fontsize=12)
    ax.set_title("Латентность операций (AVG / P50 / P95 / P99)", fontsize=14, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(names, rotation=15, ha="right")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{RESULTS_DIR}/latency_chart.png", dpi=150)
    plt.close()
    print(f"  📊  Saved {RESULTS_DIR}/latency_chart.png")


def plot_latency_distribution(results: list[dict]):
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    axes = axes.flatten()
    for i, r in enumerate(results):
        if i >= len(axes):
            break
        ax = axes[i]
        lats = r["latencies"]
        ax.hist(lats, bins=50, color="#5C6BC0", edgecolor="white", alpha=0.85)
        ax.axvline(statistics.mean(lats), color="red", linestyle="--", label=f"AVG={statistics.mean(lats):.1f}ms")
        ax.set_title(r["name"], fontsize=11)
        ax.set_xlabel("мс")
        ax.set_ylabel("Кол-во")
        ax.legend(fontsize=8)
    for j in range(len(results), len(axes)):
        axes[j].set_visible(False)
    plt.suptitle("Распределение латентности по операциям", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(f"{RESULTS_DIR}/latency_distribution.png", dpi=150)
    plt.close()
    print(f"  📊  Saved {RESULTS_DIR}/latency_distribution.png")


def plot_concurrent(concurrent_results: list[dict]):
    threads = [r["threads"] for r in concurrent_results]
    ops_sec = [r["ops_per_sec"] for r in concurrent_results]
    avg_lat = [r["avg_latency_ms"] for r in concurrent_results]

    fig, ax1 = plt.subplots(figsize=(10, 6))
    color1 = "#2196F3"
    color2 = "#F44336"

    ax1.plot(threads, ops_sec, "o-", color=color1, linewidth=2, markersize=8, label="Ops/sec")
    ax1.set_xlabel("Потоки", fontsize=12)
    ax1.set_ylabel("Операций / сек", color=color1, fontsize=12)
    ax1.tick_params(axis="y", labelcolor=color1)

    ax2 = ax1.twinx()
    ax2.plot(threads, avg_lat, "s--", color=color2, linewidth=2, markersize=8, label="Avg latency")
    ax2.set_ylabel("Ср. латентность (мс)", color=color2, fontsize=12)
    ax2.tick_params(axis="y", labelcolor=color2)

    ax1.set_title("Масштабирование при конкурентной нагрузке", fontsize=14, fontweight="bold")
    ax1.set_xticks(threads)
    ax1.grid(alpha=0.3)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    plt.tight_layout()
    plt.savefig(f"{RESULTS_DIR}/concurrent_scaling.png", dpi=150)
    plt.close()
    print(f"  📊  Saved {RESULTS_DIR}/concurrent_scaling.png")


def main():
    print("=" * 60)
    print("LOAD TEST — MongoDB Sharded Cluster")
    print("=" * 60)

    print("\n  Preloading student IDs...")
    ids = preload_ids(10_000)
    if len(ids) < 100:
        print("  Not enough data. Run seed_data.py first!")
        return
    print(f"  Loaded {len(ids)} IDs for testing.\n")

    all_results = []

    # 1) Bulk INSERT
    print("── Test 1: Bulk INSERT ──")
    r = test_bulk_insert(NUM_OPS)
    all_results.append(r)
    print(f"   {r['ops_per_sec']:,.0f} ops/s, avg={r['avg_latency_ms']:.2f}ms\n")

    # 2) Read by shard key
    print("── Test 2: READ (shard key) ──")
    r = test_read_by_shard_key(ids, NUM_OPS)
    all_results.append(r)
    print(f"   {r['ops_per_sec']:,.0f} ops/s, avg={r['avg_latency_ms']:.2f}ms\n")

    # 3) Read scatter-gather
    print("── Test 3: READ (scatter-gather) ──")
    r = test_read_scatter_gather(NUM_OPS)
    all_results.append(r)
    print(f"   {r['ops_per_sec']:,.0f} ops/s, avg={r['avg_latency_ms']:.2f}ms\n")

    # 4) Update by shard key
    print("── Test 4: UPDATE (shard key) ──")
    r = test_update_by_shard_key(ids, NUM_OPS)
    all_results.append(r)
    print(f"   {r['ops_per_sec']:,.0f} ops/s, avg={r['avg_latency_ms']:.2f}ms\n")

    # 5) Aggregation
    print("── Test 5: Aggregation pipeline ──")
    r = test_aggregation(50)  # fewer iterations — heavy query
    all_results.append(r)
    print(f"   {r['ops_per_sec']:,.0f} ops/s, avg={r['avg_latency_ms']:.2f}ms\n")

    # 6) Mixed workload
    print("── Test 6: Mixed workload (70R/20U/10I) ──")
    r = test_mixed_workload(ids, NUM_OPS)
    all_results.append(r)
    print(f"   {r['ops_per_sec']:,.0f} ops/s, avg={r['avg_latency_ms']:.2f}ms\n")

    # 7) Concurrent scaling
    print("── Test 7: Concurrent scaling ──")
    concurrent_results = test_concurrent(ids, ops_total=2000)

    # ── Summary table ──
    print("\n" + "=" * 80)
    print(" SUMMARY")
    print("=" * 80)
    table = []
    for r in all_results:
        table.append([
            r["name"],
            r["ops"],
            f"{r['total_sec']:.2f}",
            f"{r['ops_per_sec']:,.0f}",
            f"{r['avg_latency_ms']:.2f}",
            f"{r['p50_ms']:.2f}",
            f"{r['p95_ms']:.2f}",
            f"{r['p99_ms']:.2f}",
        ])
    headers = ["Тест", "Операций", "Время(с)", "Ops/s", "Avg(мс)", "P50(мс)", "P95(мс)", "P99(мс)"]
    print(tabulate(table, headers=headers, tablefmt="rounded_grid"))

    # ── Save JSON ──
    json_results = []
    for r in all_results:
        jr = {k: v for k, v in r.items() if k != "latencies"}
        json_results.append(jr)
    json_results.append({"concurrent_scaling": concurrent_results})

    with open(f"{RESULTS_DIR}/load_test_results.json", "w") as f:
        json.dump(json_results, f, indent=2, default=str)
    print(f"\n   Saved {RESULTS_DIR}/load_test_results.json")

    # ── Charts ──
    print("\n  Generating charts...")
    plot_throughput(all_results)
    plot_latency(all_results)
    plot_latency_distribution(all_results)
    plot_concurrent(concurrent_results)

    print("\n  Load test complete!")


if __name__ == "__main__":
    main()

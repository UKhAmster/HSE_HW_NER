from db import get_db

def main():
    db = get_db()
    admin = db.client.admin

    print("=" * 60)
    print("  SHARD DISTRIBUTION REPORT")
    print("=" * 60)

    # Shards
    shards = admin.command("listShards")
    print("\n📦 Шарды кластера:")
    for s in shards.get("shards", []):
        print(f"  • {s['_id']}: {s['host']}")

    # Distribution
    print("\n📊 Распределение документов:\n")
    for col_name in ["students", "courses", "enrollments", "grades"]:
        try:
            stats = db.command("collStats", col_name)
            count = stats.get("count", 0)
            sharded = stats.get("sharded", False)
            print(f"  {col_name}: {count:,} документов (sharded={sharded})")
            shard_info = stats.get("shards", {})
            for shard_name, shard_data in shard_info.items():
                sc = shard_data.get("count", 0)
                pct = sc / max(count, 1) * 100
                bar = "█" * int(pct / 2)
                print(f"    └─ {shard_name}: {sc:>10,} ({pct:5.1f}%) {bar}")
            print()
        except Exception as e:
            print(f"  {col_name}: error — {e}\n")

    # Chunk distribution
    print("📦 Chunk distribution:")
    config_db = db.client.config
    for col_name in ["students", "courses", "enrollments", "grades"]:
        ns = f"university_db.{col_name}"
        chunks = list(config_db.chunks.find({"ns": ns}))
        shard_chunks = {}
        for chunk in chunks:
            shard = chunk.get("shard", "unknown")
            shard_chunks[shard] = shard_chunks.get(shard, 0) + 1
        total_chunks = len(chunks)
        print(f"\n  {col_name}: {total_chunks} chunks")
        for shard, n in sorted(shard_chunks.items()):
            print(f"    └─ {shard}: {n} chunks ({n/max(total_chunks,1)*100:.1f}%)")


if __name__ == "__main__":
    main()

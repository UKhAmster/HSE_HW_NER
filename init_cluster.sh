set -euo pipefail

echo "=== [1/5] Initializing Config Server Replica Set ==="
docker exec configsvr1 mongosh --port 27019 --eval '
rs.initiate({
  _id: "cfgrs",
  configsvr: true,
  members: [
    { _id: 0, host: "configsvr1:27019" },
    { _id: 1, host: "configsvr2:27019" },
    { _id: 2, host: "configsvr3:27019" }
  ]
})'

sleep 5

echo "=== [2/5] Initializing Shard 1 Replica Set ==="
docker exec shard1svr1 mongosh --port 27018 --eval '
rs.initiate({
  _id: "shard1rs",
  members: [
    { _id: 0, host: "shard1svr1:27018" },
    { _id: 1, host: "shard1svr2:27018" },
    { _id: 2, host: "shard1svr3:27018" }
  ]
})'

sleep 5

echo "=== [3/5] Initializing Shard 2 Replica Set ==="
docker exec shard2svr1 mongosh --port 27018 --eval '
rs.initiate({
  _id: "shard2rs",
  members: [
    { _id: 0, host: "shard2svr1:27018" },
    { _id: 1, host: "shard2svr2:27018" },
    { _id: 2, host: "shard2svr3:27018" }
  ]
})'

sleep 10

echo "=== [4/5] Adding Shards to Cluster via mongos ==="
docker exec mongos mongosh --eval '
sh.addShard("shard1rs/shard1svr1:27018,shard1svr2:27018,shard1svr3:27018");
sh.addShard("shard2rs/shard2svr1:27018,shard2svr2:27018,shard2svr3:27018");
'

sleep 3

echo "=== [5/5] Enabling Sharding on university_db ==="
docker exec mongos mongosh --eval '
sh.enableSharding("university_db");

// --- students: hashed shard key on student_id ---
db = db.getSiblingDB("university_db");
db.createCollection("students");
db.students.createIndex({ student_id: "hashed" });
sh.shardCollection("university_db.students", { student_id: "hashed" });

// --- courses: hashed shard key on course_id ---
db.createCollection("courses");
db.courses.createIndex({ course_id: "hashed" });
sh.shardCollection("university_db.courses", { course_id: "hashed" });

// --- enrollments: compound shard key (student_id hashed) ---
db.createCollection("enrollments");
db.enrollments.createIndex({ student_id: "hashed" });
sh.shardCollection("university_db.enrollments", { student_id: "hashed" });

// --- grades: hashed shard key on student_id ---
db.createCollection("grades");
db.grades.createIndex({ student_id: "hashed" });
sh.shardCollection("university_db.grades", { student_id: "hashed" });

print("=== Sharding configured successfully! ===");
sh.status();
'

echo ""
echo " Cluster is ready.  Connect via:  mongosh mongodb://localhost:27017"

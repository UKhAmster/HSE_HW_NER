"""
web_app.py — Flask web interface for university_db (bonus feature)

Run:  python web_app.py
Open:  http://localhost:5000
"""
from __future__ import annotations

import uuid
from datetime import datetime

from bson import ObjectId, Timestamp
from flask import Flask, jsonify, render_template_string, request
from flask.json.provider import DefaultJSONProvider

from db import DEGREE_PROGRAMS, FACULTIES, get_db


class MongoJSONProvider(DefaultJSONProvider):
    """Handle MongoDB-specific types (ObjectId, Timestamp, datetime)."""
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, Timestamp):
            return {"t": o.time, "i": o.inc}
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


app = Flask(__name__)
app.json_provider_class = MongoJSONProvider
app.json = MongoJSONProvider(app)
db = get_db()


def uid() -> str:
    return uuid.uuid4().hex[:12]


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>University DB — MongoDB Sharded</title>
<style>
  :root { --primary: #1a73e8; --bg: #f8f9fa; --card: #fff; --text: #202124; --border: #dadce0; }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); }
  .navbar { background: var(--primary); color: #fff; padding: 16px 32px; font-size: 20px; font-weight: 600;
            display: flex; align-items: center; gap: 12px; }
  .navbar span { font-size: 14px; opacity: 0.8; font-weight: 400; }
  .container { max-width: 1200px; margin: 24px auto; padding: 0 16px; }
  .tabs { display: flex; gap: 8px; margin-bottom: 20px; flex-wrap: wrap; }
  .tab { padding: 10px 20px; background: var(--card); border: 1px solid var(--border); border-radius: 8px;
         cursor: pointer; font-size: 14px; transition: all .15s; }
  .tab:hover { background: #e8f0fe; }
  .tab.active { background: var(--primary); color: #fff; border-color: var(--primary); }
  .card { background: var(--card); border-radius: 12px; border: 1px solid var(--border);
          padding: 24px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,.08); }
  .card h2 { font-size: 18px; margin-bottom: 16px; color: var(--primary); }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { background: #f1f3f4; text-align: left; padding: 10px 12px; font-weight: 600; border-bottom: 2px solid var(--border); }
  td { padding: 8px 12px; border-bottom: 1px solid var(--border); }
  tr:hover td { background: #f8f9ff; }
  .form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  label { font-size: 13px; font-weight: 500; display: block; margin-bottom: 4px; }
  input, select { width: 100%; padding: 8px 12px; border: 1px solid var(--border); border-radius: 6px; font-size: 14px; }
  .btn { padding: 10px 24px; background: var(--primary); color: #fff; border: none; border-radius: 6px;
         cursor: pointer; font-size: 14px; font-weight: 500; transition: background .15s; }
  .btn:hover { background: #1557b0; }
  .btn-danger { background: #d93025; }
  .btn-danger:hover { background: #b3261e; }
  .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; }
  .stat-card { background: #e8f0fe; border-radius: 10px; padding: 20px; text-align: center; }
  .stat-card .value { font-size: 28px; font-weight: 700; color: var(--primary); }
  .stat-card .label { font-size: 13px; color: #5f6368; margin-top: 4px; }
  .search-bar { display: flex; gap: 8px; margin-bottom: 16px; }
  .search-bar input { flex: 1; }
  .hidden { display: none; }
  .alert { padding: 12px 16px; border-radius: 8px; margin-bottom: 16px; font-size: 14px; }
  .alert-success { background: #e6f4ea; color: #1e8e3e; }
  .alert-error { background: #fce8e6; color: #d93025; }
  .shard-bar { display: flex; height: 32px; border-radius: 6px; overflow: hidden; margin: 8px 0; }
  .shard-bar div { display: flex; align-items: center; justify-content: center; color: #fff;
                   font-size: 12px; font-weight: 600; }
</style>
</head>
<body>
<div class="navbar"> University DB <span>MongoDB Sharded Cluster</span></div>
<div class="container">
  <div class="tabs">
    <div class="tab active" onclick="showTab('students')">Студенты</div>
    <div class="tab" onclick="showTab('add')">Добавить</div>
    <div class="tab" onclick="showTab('analytics')">Аналитика</div>
    <div class="tab" onclick="showTab('sharding')">Шардинг</div>
  </div>

  <div id="alert-area"></div>

  <!-- Students tab -->
  <div id="tab-students" class="card">
    <h2>Список студентов</h2>
    <div class="search-bar">
      <input type="text" id="search-input" placeholder="Поиск по фамилии..." onkeyup="if(event.key==='Enter')searchStudents()">
      <select id="search-faculty" style="width:auto">
        <option value="">Все факультеты</option>
        {% for f in faculties %}<option value="{{f}}">{{f}}</option>{% endfor %}
      </select>
      <button class="btn" onclick="searchStudents()">Найти</button>
    </div>
    <div id="students-table">Загрузка...</div>
  </div>

  <!-- Add tab -->
  <div id="tab-add" class="card hidden">
    <h2>Добавить студента</h2>
    <div class="form-grid">
      <div><label>Фамилия</label><input id="f-last" placeholder="Иванов"></div>
      <div><label>Имя</label><input id="f-first" placeholder="Иван"></div>
      <div><label>Отчество</label><input id="f-patron" placeholder="Иванович"></div>
      <div><label>Email</label><input id="f-email" placeholder="ivanov@mail.ru"></div>
      <div><label>Телефон</label><input id="f-phone" placeholder="+79001234567"></div>
      <div><label>Факультет</label>
        <select id="f-faculty">{% for f in faculties %}<option>{{f}}</option>{% endfor %}</select>
      </div>
      <div><label>Программа</label>
        <select id="f-program">{% for p in programs %}<option>{{p}}</option>{% endfor %}</select>
      </div>
      <div><label>Курс</label><input id="f-year" type="number" value="1" min="1" max="6"></div>
    </div>
    <br>
    <button class="btn" onclick="addStudent()">Добавить</button>
  </div>

  <!-- Analytics tab -->
  <div id="tab-analytics" class="card hidden">
    <h2>Аналитика</h2>
    <div id="analytics-content">Загрузка...</div>
  </div>

  <!-- Sharding tab -->
  <div id="tab-sharding" class="card hidden">
    <h2>Информация о шардировании</h2>
    <div id="sharding-content">Загрузка...</div>
  </div>
</div>

<script>
function showTab(name) {
  document.querySelectorAll('.card').forEach(c => c.classList.add('hidden'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('tab-' + name).classList.remove('hidden');
  event.target.classList.add('active');
  if (name === 'students') searchStudents();
  if (name === 'analytics') loadAnalytics();
  if (name === 'sharding') loadSharding();
}

function showAlert(msg, type='success') {
  const area = document.getElementById('alert-area');
  area.innerHTML = `<div class="alert alert-${type}">${msg}</div>`;
  setTimeout(() => area.innerHTML = '', 4000);
}

async function searchStudents() {
  const name = document.getElementById('search-input').value;
  const fac = document.getElementById('search-faculty').value;
  const params = new URLSearchParams();
  if (name) params.append('name', name);
  if (fac) params.append('faculty', fac);
  const res = await fetch('/api/students?' + params);
  const data = await res.json();
  let html = '<table><tr><th>ID</th><th>Фамилия</th><th>Имя</th><th>Факультет</th><th>Программа</th><th>Курс</th><th>Действия</th></tr>';
  data.forEach(s => {
    html += `<tr><td>${s.student_id}</td><td>${s.last_name}</td><td>${s.first_name}</td>` +
            `<td>${s.faculty}</td><td>${s.program}</td><td>${s.year}</td>` +
            `<td><button class="btn btn-danger" style="padding:4px 12px;font-size:12px" ` +
            `onclick="deleteStudent('${s.student_id}')">Удалить</button></td></tr>`;
  });
  html += '</table>';
  if (!data.length) html = '<p>Студенты не найдены.</p>';
  document.getElementById('students-table').innerHTML = html;
}

async function addStudent() {
  const body = {
    last_name: document.getElementById('f-last').value,
    first_name: document.getElementById('f-first').value,
    patronymic: document.getElementById('f-patron').value || null,
    email: document.getElementById('f-email').value,
    phone: document.getElementById('f-phone').value,
    faculty: document.getElementById('f-faculty').value,
    program: document.getElementById('f-program').value,
    year: parseInt(document.getElementById('f-year').value),
  };
  const res = await fetch('/api/students', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
  const data = await res.json();
  if (data.ok) { showAlert('Студент добавлен: ' + data.student_id); showTab('students'); }
  else showAlert('Ошибка: ' + data.error, 'error');
}

async function deleteStudent(sid) {
  if (!confirm('Удалить студента ' + sid + '?')) return;
  const res = await fetch('/api/students/' + sid, {method:'DELETE'});
  const data = await res.json();
  if (data.ok) { showAlert('Студент удалён'); searchStudents(); }
  else showAlert('Ошибка', 'error');
}

async function loadAnalytics() {
  const res = await fetch('/api/analytics');
  const data = await res.json();
  let html = '<div class="stats-grid">';
  html += `<div class="stat-card"><div class="value">${data.total_students.toLocaleString()}</div><div class="label">Студентов</div></div>`;
  html += `<div class="stat-card"><div class="value">${data.total_courses.toLocaleString()}</div><div class="label">Курсов</div></div>`;
  html += `<div class="stat-card"><div class="value">${data.total_enrollments.toLocaleString()}</div><div class="label">Записей</div></div>`;
  html += `<div class="stat-card"><div class="value">${data.total_grades.toLocaleString()}</div><div class="label">Оценок</div></div>`;
  html += '</div>';
  html += '<h3 style="margin:20px 0 10px">Средний балл по факультетам</h3><table><tr><th>Факультет</th><th>Ср. балл</th><th>Оценок</th></tr>';
  data.avg_by_faculty.forEach(r => {
    html += `<tr><td>${r.faculty}</td><td>${r.avg.toFixed(2)}</td><td>${r.count.toLocaleString()}</td></tr>`;
  });
  html += '</table>';
  document.getElementById('analytics-content').innerHTML = html;
}

async function loadSharding() {
  const res = await fetch('/api/sharding');
  const data = await res.json();
  let html = '<h3>Шарды</h3><table><tr><th>ID</th><th>Host</th></tr>';
  data.shards.forEach(s => { html += `<tr><td>${s._id}</td><td>${s.host}</td></tr>`; });
  html += '</table>';
  html += '<h3 style="margin-top:20px">Распределение документов</h3>';
  const colors = ['#1a73e8', '#34a853', '#ea4335', '#fbbc04'];
  data.collections.forEach(c => {
    html += `<div style="margin:12px 0"><strong>${c.name}</strong>: ${c.count.toLocaleString()} docs`;
    if (c.shards && c.shards.length > 1) {
      const total = c.shards.reduce((a, s) => a + s.count, 0) || 1;
      html += '<div class="shard-bar">';
      c.shards.forEach((s, i) => {
        const pct = (s.count / total * 100).toFixed(1);
        html += `<div style="width:${pct}%;background:${colors[i%colors.length]}">${s.name}: ${pct}%</div>`;
      });
      html += '</div>';
    }
    html += '</div>';
  });
  document.getElementById('sharding-content').innerHTML = html;
}

// Initial load
searchStudents();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE, faculties=FACULTIES, programs=DEGREE_PROGRAMS)


@app.route("/api/students", methods=["GET"])
def api_list_students():
    query = {}
    name = request.args.get("name")
    faculty = request.args.get("faculty")
    if name:
        query["last_name"] = {"$regex": name, "$options": "i"}
    if faculty:
        query["faculty"] = faculty
    results = list(db.students.find(query, {"_id": 0}).limit(50))
    return jsonify(results)


@app.route("/api/students", methods=["POST"])
def api_add_student():
    data = request.json
    doc = {
        "student_id": uid(),
        "last_name": data.get("last_name", ""),
        "first_name": data.get("first_name", ""),
        "patronymic": data.get("patronymic"),
        "email": data.get("email", ""),
        "phone": data.get("phone", ""),
        "faculty": data.get("faculty", FACULTIES[0]),
        "program": data.get("program", DEGREE_PROGRAMS[0]),
        "year": data.get("year", 1),
        "enrollment_date": datetime.utcnow(),
        "is_active": True,
        "created_at": datetime.utcnow(),
    }
    db.students.insert_one(doc)
    return jsonify({"ok": True, "student_id": doc["student_id"]})


@app.route("/api/students/<sid>", methods=["DELETE"])
def api_delete_student(sid):
    db.students.delete_one({"student_id": sid})
    db.enrollments.delete_many({"student_id": sid})
    db.grades.delete_many({"student_id": sid})
    return jsonify({"ok": True})


@app.route("/api/analytics")
def api_analytics():
    pipeline = [
        {"$lookup": {"from": "students", "localField": "student_id",
                      "foreignField": "student_id", "as": "st"}},
        {"$unwind": "$st"},
        {"$group": {"_id": "$st.faculty", "avg": {"$avg": "$grade"}, "count": {"$sum": 1}}},
        {"$sort": {"avg": -1}},
    ]
    avg_by_fac = [{"faculty": r["_id"], "avg": r["avg"], "count": r["count"]}
                  for r in db.grades.aggregate(pipeline, allowDiskUse=True)]
    return jsonify({
        "total_students": db.students.estimated_document_count(),
        "total_courses": db.courses.estimated_document_count(),
        "total_enrollments": db.enrollments.estimated_document_count(),
        "total_grades": db.grades.estimated_document_count(),
        "avg_by_faculty": avg_by_fac,
    })


@app.route("/api/sharding")
def api_sharding():
    admin = db.client.admin
    raw_shards = admin.command("listShards").get("shards", [])
    shards_info = [{"_id": s["_id"], "host": s["host"]} for s in raw_shards]
    collections_info = []
    for col_name in ["students", "courses", "enrollments", "grades"]:
        try:
            stats = db.command("collStats", col_name)
            shard_details = []
            for sname, sdata in stats.get("shards", {}).items():
                shard_details.append({"name": sname, "count": sdata.get("count", 0)})
            collections_info.append({
                "name": col_name,
                "count": stats.get("count", 0),
                "sharded": stats.get("sharded", False),
                "shards": shard_details,
            })
        except Exception:
            collections_info.append({"name": col_name, "count": 0, "shards": []})
    return jsonify({"shards": shards_info, "collections": collections_info})


if __name__ == "__main__":
    print("🌐  Starting web interface at http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)

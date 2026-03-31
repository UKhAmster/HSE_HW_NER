from __future__ import annotations

import sys
import uuid
from datetime import datetime
from textwrap import dedent

from tabulate import tabulate

from db import DEGREE_PROGRAMS, FACULTIES, get_db

db = get_db()


def uid() -> str:
    return uuid.uuid4().hex[:12]


def input_int(prompt: str, default: int | None = None) -> int:
    while True:
        raw = input(prompt).strip()
        if not raw and default is not None:
            return default
        try:
            return int(raw)
        except ValueError:
            print("  ⚠  Введите целое число.")


def print_table(rows: list[dict], keys: list[str] | None = None):
    if not rows:
        print("  (нет данных)")
        return
    if keys is None:
        keys = list(rows[0].keys())
    # Remove _id from display
    keys = [k for k in keys if k != "_id"]
    table = [[r.get(k, "") for k in keys] for r in rows]
    print(tabulate(table, headers=keys, tablefmt="rounded_grid"))


def add_student():
    print("\n── Добавление студента ──")
    sid = uid()
    last_name = input("  Фамилия: ").strip()
    first_name = input("  Имя: ").strip()
    patronymic = input("  Отчество (Enter — пропустить): ").strip() or None
    email = input("  Email: ").strip()
    phone = input("  Телефон: ").strip()

    print("  Факультеты:")
    for i, f in enumerate(FACULTIES, 1):
        print(f"    {i}. {f}")
    fac_idx = input_int("  Номер факультета: ", 1) - 1
    faculty = FACULTIES[fac_idx % len(FACULTIES)]

    print("  Программы: " + ", ".join(f"{i+1}.{p}" for i, p in enumerate(DEGREE_PROGRAMS)))
    prog_idx = input_int("  Номер программы: ", 1) - 1
    program = DEGREE_PROGRAMS[prog_idx % len(DEGREE_PROGRAMS)]

    year = input_int("  Курс (1-6): ", 1)

    doc = {
        "student_id": sid,
        "last_name": last_name,
        "first_name": first_name,
        "patronymic": patronymic,
        "email": email,
        "phone": phone,
        "faculty": faculty,
        "program": program,
        "year": year,
        "enrollment_date": datetime.utcnow(),
        "is_active": True,
        "created_at": datetime.utcnow(),
    }
    db.students.insert_one(doc)
    print(f"\n  ✅  Студент добавлен (student_id={sid})")


def find_students():
    print("\n── Поиск студентов ──")
    print("  1. По фамилии")
    print("  2. По факультету")
    print("  3. По student_id")
    print("  4. Все (лимит 20)")
    choice = input_int("  Выбор: ", 4)

    query = {}
    if choice == 1:
        name = input("  Фамилия (или часть): ").strip()
        query = {"last_name": {"$regex": name, "$options": "i"}}
    elif choice == 2:
        for i, f in enumerate(FACULTIES, 1):
            print(f"    {i}. {f}")
        idx = input_int("  Номер: ", 1) - 1
        query = {"faculty": FACULTIES[idx % len(FACULTIES)]}
    elif choice == 3:
        sid = input("  student_id: ").strip()
        query = {"student_id": sid}

    results = list(db.students.find(query).limit(20))
    print(f"\n  Найдено: {len(results)}")
    print_table(
        results,
        ["student_id", "last_name", "first_name", "faculty", "program", "year", "is_active"],
    )


def update_student():
    print("\n── Обновление студента ──")
    sid = input("  student_id: ").strip()
    student = db.students.find_one({"student_id": sid})
    if not student:
        print("  ⚠  Студент не найден.")
        return

    print(f"  Текущие данные: {student['last_name']} {student['first_name']}, "
          f"факультет={student['faculty']}, курс={student['year']}")
    print("  Что обновить?")
    print("  1. Курс")
    print("  2. Факультет")
    print("  3. Статус (активен/отчислен)")
    choice = input_int("  Выбор: ", 1)

    update = {}
    if choice == 1:
        new_year = input_int("  Новый курс: ")
        update = {"$set": {"year": new_year}}
    elif choice == 2:
        for i, f in enumerate(FACULTIES, 1):
            print(f"    {i}. {f}")
        idx = input_int("  Номер: ", 1) - 1
        update = {"$set": {"faculty": FACULTIES[idx % len(FACULTIES)]}}
    elif choice == 3:
        active = input("  Активен? (y/n): ").strip().lower() == "y"
        update = {"$set": {"is_active": active}}

    if update:
        db.students.update_one({"student_id": sid}, update)
        print("  ✅  Обновлено.")


def delete_student():
    print("\n── Удаление студента ──")
    sid = input("  student_id: ").strip()
    result = db.students.delete_one({"student_id": sid})
    if result.deleted_count:
        print(" Удалён.")
        # Also clean up enrollments and grades
        db.enrollments.delete_many({"student_id": sid})
        db.grades.delete_many({"student_id": sid})
        print("Связанные записи тоже удалены.")
    else:
        print("Студент не найден.")


def aggregation_menu():
    print("\n── Аналитика ──")
    print("  1. Средний балл по факультетам")
    print("  2. Количество студентов по факультетам")
    print("  3. Топ-10 студентов по среднему баллу")
    print("  4. Распределение по программам обучения")
    choice = input_int("  Выбор: ", 1)

    if choice == 1:
        pipeline = [
            {"$lookup": {
                "from": "students",
                "localField": "student_id",
                "foreignField": "student_id",
                "as": "student"
            }},
            {"$unwind": "$student"},
            {"$group": {
                "_id": "$student.faculty",
                "avg_grade": {"$avg": "$grade"},
                "total_grades": {"$sum": 1},
            }},
            {"$sort": {"avg_grade": -1}},
        ]
        results = list(db.grades.aggregate(pipeline, allowDiskUse=True))
        rows = [{"Факультет": r["_id"], "Ср. балл": f"{r['avg_grade']:.2f}",
                 "Оценок": r["total_grades"]} for r in results]
        print_table(rows)

    elif choice == 2:
        pipeline = [
            {"$group": {"_id": "$faculty", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
        results = list(db.students.aggregate(pipeline))
        rows = [{"Факультет": r["_id"], "Студентов": r["count"]} for r in results]
        print_table(rows)

    elif choice == 3:
        pipeline = [
            {"$group": {
                "_id": "$student_id",
                "avg_grade": {"$avg": "$grade"},
                "n_grades": {"$sum": 1},
            }},
            {"$match": {"n_grades": {"$gte": 3}}},
            {"$sort": {"avg_grade": -1}},
            {"$limit": 10},
            {"$lookup": {
                "from": "students",
                "localField": "_id",
                "foreignField": "student_id",
                "as": "student"
            }},
            {"$unwind": "$student"},
        ]
        results = list(db.grades.aggregate(pipeline, allowDiskUse=True))
        rows = [{
            "student_id": r["_id"],
            "ФИО": f"{r['student']['last_name']} {r['student']['first_name']}",
            "Ср. балл": f"{r['avg_grade']:.2f}",
            "Оценок": r["n_grades"],
        } for r in results]
        print_table(rows)

    elif choice == 4:
        pipeline = [
            {"$group": {"_id": "$program", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
        results = list(db.students.aggregate(pipeline))
        rows = [{"Программа": r["_id"], "Студентов": r["count"]} for r in results]
        print_table(rows)


def shard_info():
    print("\n── Информация о шардировании ──")
    admin = db.client.admin

    # List shards
    shards = admin.command("listShards")
    print("\nШарды:")
    for s in shards.get("shards", []):
        print(f"  {s['_id']}: {s['host']} (state={s.get('state', 'N/A')})")

    # Collection distribution
    print("\nРаспределение данных по коллекциям:")
    for col_name in ["students", "courses", "enrollments", "grades"]:
        try:
            stats = db.command("collStats", col_name)
            count = stats.get("count", 0)
            sharded = stats.get("sharded", False)
            shard_info_dict = stats.get("shards", {})
            print(f"\n  {col_name}: {count:,} документов, sharded={sharded}")
            for shard_name, shard_stats in shard_info_dict.items():
                sc = shard_stats.get("count", 0)
                print(f"    └─ {shard_name}: {sc:,} документов ({sc/max(count,1)*100:.1f}%)")
        except Exception as e:
            print(f"  {col_name}: ошибка — {e}")


MENU = dedent("""\
    ╔══════════════════════════════════════════╗
    ║   University DB — Консольный интерфейс   ║
    ╠══════════════════════════════════════════╣
    ║  1. Добавить студента                    ║
    ║  2. Найти студентов                      ║
    ║  3. Обновить студента                    ║
    ║  4. Удалить студента                     ║
    ║  5. Аналитика (агрегации)                ║
    ║  6. Информация о шардировании            ║
    ║  0. Выход                                ║
    ╚══════════════════════════════════════════╝
""")

ACTIONS = {
    1: add_student,
    2: find_students,
    3: update_student,
    4: delete_student,
    5: aggregation_menu,
    6: shard_info,
}


def main():
    print("\n🎓  Подключение к MongoDB...")
    try:
        db.client.admin.command("ping")
        print(" Подключено.\n")
    except Exception as e:
        print(f" Ошибка подключения: {e}")
        sys.exit(1)

    while True:
        print(MENU)
        choice = input_int("Выбор: ", 0)
        if choice == 0:
            print("До свидания!")
            break
        action = ACTIONS.get(choice)
        if action:
            action()
        else:
            print("Неизвестная команда.")


if __name__ == "__main__":
    main()

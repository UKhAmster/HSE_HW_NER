from __future__ import annotations

import random
import uuid
from datetime import datetime

from faker import Faker
from tqdm import tqdm

from db import (
    DEGREE_PROGRAMS,
    FACULTIES,
    SEMESTERS,
    course_doc,
    enrollment_doc,
    get_db,
    grade_doc,
    student_doc,
)

fake = Faker("ru_RU")

# ---------- tunables ----------
NUM_STUDENTS = 50_000
NUM_COURSES = 500
ENROLLMENTS_PER_STUDENT = (3, 8)  # min, max courses per student
BATCH_SIZE = 5_000


def uid() -> str:
    return uuid.uuid4().hex[:12]


def generate_students(n: int) -> list[dict]:
    docs = []
    for _ in tqdm(range(n), desc="Generating students"):
        docs.append(
            student_doc(
                student_id=uid(),
                last_name=fake.last_name(),
                first_name=fake.first_name(),
                patronymic=fake.middle_name() if random.random() > 0.1 else None,
                email=fake.email(),
                phone=fake.phone_number(),
                faculty=random.choice(FACULTIES),
                program=random.choice(DEGREE_PROGRAMS),
                year=random.randint(1, 6),
                enrollment_date=fake.date_time_between(
                    start_date="-6y", end_date="now"
                ),
                is_active=random.random() > 0.05,
            )
        )
    return docs


def generate_courses(n: int) -> list[dict]:
    subjects = [
        "Алгоритмы и структуры данных",
        "Линейная алгебра",
        "Математический анализ",
        "Базы данных",
        "Машинное обучение",
        "Теория вероятностей",
        "Дискретная математика",
        "Операционные системы",
        "Компьютерные сети",
        "Микроэкономика",
        "Макроэкономика",
        "Эконометрика",
        "Гражданское право",
        "Уголовное право",
        "Философия",
        "Социология",
        "Иностранный язык",
        "Физика",
        "Программная инженерия",
        "Распределённые системы",
        "Нереляционные базы данных",
        "Облачные вычисления",
        "DevOps практики",
        "Информационная безопасность",
        "Цифровой маркетинг",
    ]
    docs = []
    for i in tqdm(range(n), desc="Generating courses"):
        title = random.choice(subjects) + f" (поток {i % 10 + 1})"
        docs.append(
            course_doc(
                course_id=uid(),
                title=title,
                faculty=random.choice(FACULTIES),
                credits=random.choice([2, 3, 4, 5, 6]),
                semester=random.choice(SEMESTERS),
                year=random.randint(2020, 2026),
                instructor=fake.name(),
                max_students=random.choice([50, 100, 150, 200, 300]),
            )
        )
    return docs


def generate_enrollments_and_grades(
    student_ids: list[str], course_ids: list[str]
) -> tuple[list[dict], list[dict]]:
    enrollments = []
    grades = []
    for sid in tqdm(student_ids, desc="Generating enrollments & grades"):
        n_courses = random.randint(*ENROLLMENTS_PER_STUDENT)
        chosen = random.sample(course_ids, min(n_courses, len(course_ids)))
        for cid in chosen:
            sem = random.choice(SEMESTERS)
            yr = random.randint(2020, 2026)
            eid = uid()
            enrollments.append(enrollment_doc(eid, sid, cid, sem, yr))
            # 90 % chance of having a grade
            if random.random() < 0.9:
                grades.append(
                    grade_doc(uid(), sid, cid, random.randint(1, 10), sem, yr)
                )
    return enrollments, grades


def bulk_insert(collection, docs: list[dict], label: str):
    total = len(docs)
    for i in tqdm(range(0, total, BATCH_SIZE), desc=f"Inserting {label}"):
        batch = docs[i : i + BATCH_SIZE]
        collection.insert_many(batch, ordered=False)


def main():
    db = get_db()

    # Clear existing data
    for col in ["students", "courses", "enrollments", "grades"]:
        db[col].delete_many({})
    print("Collections cleared.\n")

    # Students
    students = generate_students(NUM_STUDENTS)
    bulk_insert(db.students, students, "students")
    student_ids = [s["student_id"] for s in students]

    # Courses
    courses = generate_courses(NUM_COURSES)
    bulk_insert(db.courses, courses, "courses")
    course_ids = [c["course_id"] for c in courses]

    # Enrollments & Grades
    enrollments, grades = generate_enrollments_and_grades(student_ids, course_ids)
    bulk_insert(db.enrollments, enrollments, "enrollments")
    bulk_insert(db.grades, grades, "grades")

    print(f"\n  Seeded:")
    print(f"   Students:    {len(students):>10,}")
    print(f"   Courses:     {len(courses):>10,}")
    print(f"   Enrollments: {len(enrollments):>10,}")
    print(f"   Grades:      {len(grades):>10,}")

    # Print shard distribution
    print("\n  Shard distribution (students):")
    pipeline = [
        {"$collStats": {"storageStats": {}}},
    ]
    # Alternative: use sh.status() via admin
    admin = db.client.admin
    result = admin.command("listShards")
    for shard in result.get("shards", []):
        print(f"   {shard['_id']}: {shard['host']}")

    print("\n  Collection stats:")
    for col_name in ["students", "courses", "enrollments", "grades"]:
        stats = db.command("collStats", col_name)
        count = stats.get("count", 0)
        sharded = stats.get("sharded", False)
        n_shards = len(stats.get("shards", {}))
        print(f"   {col_name}: {count:,} docs, sharded={sharded}, across {n_shards} shard(s)")


if __name__ == "__main__":
    main()

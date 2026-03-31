from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

from pymongo import MongoClient
from pymongo.database import Database

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "university_db")


def get_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def get_db() -> Database:
    return get_client()[DB_NAME]

FACULTIES = [
    "Компьютерные науки",
    "Экономика",
    "Математика",
    "Физика",
    "Юриспруденция",
    "Менеджмент",
    "Филология",
    "Социология",
]

DEGREE_PROGRAMS = ["Бакалавриат", "Магистратура", "Аспирантура"]

SEMESTERS = ["Осень", "Весна"]


def student_doc(
    student_id: str,
    last_name: str,
    first_name: str,
    patronymic: Optional[str],
    email: str,
    phone: str,
    faculty: str,
    program: str,
    year: int,
    enrollment_date: datetime,
    is_active: bool = True,
) -> dict:
    """Return a well-formed student document."""
    return {
        "student_id": student_id,
        "last_name": last_name,
        "first_name": first_name,
        "patronymic": patronymic,
        "email": email,
        "phone": phone,
        "faculty": faculty,
        "program": program,
        "year": year,
        "enrollment_date": enrollment_date,
        "is_active": is_active,
        "created_at": datetime.utcnow(),
    }


def course_doc(
    course_id: str,
    title: str,
    faculty: str,
    credits: int,
    semester: str,
    year: int,
    instructor: str,
    max_students: int = 200,
) -> dict:
    return {
        "course_id": course_id,
        "title": title,
        "faculty": faculty,
        "credits": credits,
        "semester": semester,
        "year": year,
        "instructor": instructor,
        "max_students": max_students,
        "created_at": datetime.utcnow(),
    }


def enrollment_doc(
    enrollment_id: str,
    student_id: str,
    course_id: str,
    semester: str,
    year: int,
) -> dict:
    return {
        "enrollment_id": enrollment_id,
        "student_id": student_id,
        "course_id": course_id,
        "semester": semester,
        "year": year,
        "enrolled_at": datetime.utcnow(),
    }


def grade_doc(
    grade_id: str,
    student_id: str,
    course_id: str,
    grade: int,  # 1-10
    semester: str,
    year: int,
) -> dict:
    return {
        "grade_id": grade_id,
        "student_id": student_id,
        "course_id": course_id,
        "grade": grade,
        "semester": semester,
        "year": year,
        "graded_at": datetime.utcnow(),
    }

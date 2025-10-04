# scripts/init_db.py
import db

if __name__ == "__main__":
    db.init_schema()
    print("DB schema OK")

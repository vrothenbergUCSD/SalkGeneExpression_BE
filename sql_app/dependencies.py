from database import SessionLocal

# Get database
def get_db():
  db = SessionLocal()
  try:
    yield db
  finally:
    db.close()

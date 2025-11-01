create-migration:
	@ alembic revision --autogenerate -m "$$COMMENT"

run-migration:
	@ alembic upgrade head
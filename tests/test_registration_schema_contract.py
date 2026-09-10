from app.domains.users.schemas import UserCreate


def test_user_create_schema_requires_name_and_accepts_production_payload_shape():
    payload = {
        "name": "Avery Recruiter",
        "email": "admin@example.com",
        "password": "secret1234",
        "timezone": "UTC",
    }

    user = UserCreate.model_validate(payload)

    assert user.name == "Avery Recruiter"
    assert user.email == "admin@example.com"
    assert user.password == "secret1234"
    assert user.timezone == "UTC"

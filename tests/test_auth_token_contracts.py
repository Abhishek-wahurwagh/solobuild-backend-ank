from app.domains.auth.schemas import UserLogin, RefreshTokenRequest, TokenPairResponse


def test_login_and_refresh_token_contracts_are_valid():
    login = UserLogin.model_validate({
        "email": "admin@example.com",
        "password": "secret1234",
    })

    refresh_request = RefreshTokenRequest.model_validate({
        "refresh_token": "refresh-token-value",
    })

    token_pair = TokenPairResponse.model_validate({
        "access_token": "access-token-value",
        "refresh_token": "refresh-token-value",
        "token_type": "bearer",
    })

    assert login.email == "admin@example.com"
    assert login.password == "secret1234"
    assert refresh_request.refresh_token == "refresh-token-value"
    assert token_pair.access_token == "access-token-value"
    assert token_pair.refresh_token == "refresh-token-value"
    assert token_pair.token_type == "bearer"

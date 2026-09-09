from scripts.check_repo_agent_readiness import check_repository


def test_repository_agent_readiness_contract() -> None:
    assert check_repository() == ()

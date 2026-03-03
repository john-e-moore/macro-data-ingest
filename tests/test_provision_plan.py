from scripts.provision_aws import _csv_to_list, build_plan


def test_csv_to_list_strips_and_filters() -> None:
    result = _csv_to_list("subnet-1, subnet-2, ,subnet-3")
    assert result == ["subnet-1", "subnet-2", "subnet-3"]


def test_build_plan_uses_env_suffix(monkeypatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-2")
    monkeypatch.setenv("AWS_ACCOUNT_ID", "123456789012")
    monkeypatch.setenv("PG_INSTANCE_CLASS", "db.t4g.micro")
    monkeypatch.setenv("PG_ALLOCATED_STORAGE", "20")

    plan = build_plan("staging")

    assert plan.env == "staging"
    assert plan.data_bucket.startswith("tlg-macro-staging-data-")
    assert plan.rds_identifier == "tlg-macro-staging-pg"
    assert plan.gha_role_name == "tlg-macro-staging-gha-role"

from pathlib import Path


def test_apply_workflow_ssm_pip_installer_has_timeouts_and_fallback():
    workflow = Path('.github/workflows/apply.yml').read_text()

    assert "'--default-timeout'" in workflow
    assert "'--no-input'" in workflow
    assert 'subprocess.TimeoutExpired' in workflow
    assert "'--break-system-packages'" in workflow
    assert "vertica-python==1.4.0" in workflow


def test_apply_workflow_reports_ssm_association_failures():
    workflow = Path('.github/workflows/apply.yml').read_text()
    continuation = '\\'

    assert '--name "$SSM_DOCUMENT" ' + continuation in workflow
    assert "--query 'AssociationDescription.AssociationId' " + continuation in workflow
    assert 'ssm_document=$(terraform output -raw ssm_smoke_test_document)' in workflow
    assert 'aws ssm list-associations ' + continuation in workflow
    assert 'describe-association-executions' in workflow
    assert 'describe-association-execution-targets' in workflow
    assert 'describe-instance-associations-status' in workflow


def test_apply_workflow_tails_all_cloudwatch_streams():
    workflow = Path('.github/workflows/apply.yml').read_text()

    assert 'log_stream_tokens: dict[str, str | None] = {}' in workflow
    assert 'for stream_name, token in list(log_stream_tokens.items()):' in workflow
    assert "key = (stream_name, timestamp or 0, message)" in workflow

from pathlib import Path


def test_apply_workflow_ssm_pip_installer_has_timeouts_and_fallback():
    workflow = Path('.github/workflows/apply.yml').read_text()

    assert "'--default-timeout'" in workflow
    assert "'--no-input'" in workflow
    assert 'subprocess.TimeoutExpired' in workflow
    assert "'--break-system-packages'" in workflow
    assert "vertica-python==1.4.0" in workflow

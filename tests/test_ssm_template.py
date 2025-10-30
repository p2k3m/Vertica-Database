from pathlib import Path


def test_ssm_smoke_test_netcat_uses_connect_timeout():
    template = Path('infra/ssm-smoke-test.json.tpl').read_text()

    assert template.count('nc -vz -w 5 ') == 2

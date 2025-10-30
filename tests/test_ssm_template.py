from pathlib import Path


def test_ssm_smoke_test_netcat_uses_connect_timeout():
    template = Path('infra/ssm-smoke-test.json.tpl').read_text()

    assert template.count('nc -vz -w 5 ') == 2


def test_ssm_smoke_test_pip_installs_have_timeouts():
    template = Path('infra/ssm-smoke-test.json.tpl').read_text()

    assert template.count("'--default-timeout'") == 2
    assert template.count("'30'") >= 2
    assert template.count("'--retries'") == 2
    assert template.count("'2'") >= 2
    assert template.count("'--no-input'") == 2
    assert 'timeout=120' in template
    assert 'subprocess.TimeoutExpired' in template


def test_ssm_smoke_test_sets_imds_header_correctly():
    template = Path('infra/ssm-smoke-test.json.tpl').read_text()

    assert 'IMDS_HEADER=(-H \\"X-aws-ec2-metadata-token: $TOKEN\\")' in template


def test_ssm_smoke_test_curl_commands_have_timeouts():
    template = Path('infra/ssm-smoke-test.json.tpl').read_text()

    assert template.count('--connect-timeout 2 --max-time 5') >= 3


def test_ssm_smoke_test_docker_commands_use_timeouts():
    template = Path('infra/ssm-smoke-test.json.tpl').read_text()

    assert 'timeout 60 docker --version' in template
    assert 'timeout 60 docker info' in template
    assert 'timeout 60 docker ps --format' in template
    assert 'timeout 60 docker inspect vertica_ce' in template
    assert 'timeout 60 docker logs --tail 100 vertica_ce' in template
    assert 'timeout 60 docker image ls ${vertica_image}' in template
    assert 'timeout 300 docker pull ${vertica_image}' in template


def test_ssm_smoke_test_python_connectivity_timeout():
    template = Path('infra/ssm-smoke-test.json.tpl').read_text()

    assert "timeout 300 python3 - <<'PY'" in template

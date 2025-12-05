import importlib.util
import os
import subprocess
import time

REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
COMPOSE_FILE = os.path.join(REPO_ROOT, 'docker-compose.yml')
GENERATE_SCRIPT = os.path.join(REPO_ROOT, 'generar-compose.py')

def load_generator():
    spec = importlib.util.spec_from_file_location("generar_compose", GENERATE_SCRIPT)
    gen_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen_mod)
    return gen_mod


def generate_compose_with_analysts(n=3):
    gen_mod = load_generator()
    nodes_count = {
    "filter-TbyYear": 3,
    "filter-TbyHour": 3,
    "filter-TbyAmount": 3,
    "filter-TIbyYear": 3,
    "items-aggregator": 3,
    "tpv-aggregator": 3,
    "tpv-joiner": 3,
    "topuser-aggregator": 3,
    "topuser-birthdate-joiner": 3,
    "monitor": 3,
    "analyst": n,
}
    gen_mod.generate_compose_file(COMPOSE_FILE, nodes_count)


def run_compose_up():
    print("Bringing up docker-compose services...")
    subprocess.check_call(['docker', 'compose', 'up', '-d'], cwd=REPO_ROOT)


def run_compose_down():
    print("Tearing down docker-compose services...")
    subprocess.call(['docker', 'compose', 'down', '--volumes', '--remove-orphans'], cwd=REPO_ROOT)

def find_analysts_containers(client):
    analyst_containers = [c for c in client.containers.list(all=True) if c.name.startswith('analyst-')]
    if not analyst_containers:
        raise Exception("No analyst containers found")
    print(f"Found analyst containers: {[c.name for c in analyst_containers]}")
    return analyst_containers


def wait_for_exit(container, timeout=60):
    waited = 0
    while waited < timeout:
        container.reload()
        if container.status in ('exited', 'dead'):
            state = container.attrs.get('State', {})
            return state.get('ExitCode'), state
        time.sleep(1)
        waited += 1
    return None, None

def check_system_still_running(docker_cli):
    not_analyst_node = [c for c in docker_cli.containers.list() if not c.name.startswith('analyst-')]
    for node in not_analyst_node:
        if not not_analyst_node:
            raise Exception(f"{node.name} container not found or not running")
        else:
            node.reload()
            print(f"{node.name} status: {node.status}")
            if node.status != 'running':
                raise Exception(f"{node.name} is not running (status={node.status})")
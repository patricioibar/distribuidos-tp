import os
import subprocess
import time
import yaml
import docker
import os
import subprocess
import time
import sys

# Ensure repository root is on sys.path when running this script directly so
# `from tests.utils import *` resolves.
repo_root = os.path.dirname(os.path.dirname(__file__))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from tests.utils import *

def test_simultaneous_analysts():
    generate_compose_with_analysts(3)

    try:
        run_compose_up()
    except subprocess.CalledProcessError as e:
        print(f"Failed to bring up docker-compose: {e}")
        sys.exit(3)

    try:
        docker_cli = docker.from_env()
    except Exception as e:
        print(f"Could not connect to Docker daemon: {e}")
        run_compose_down()
        sys.exit(4)

    success = True
    try:
        # give containers time to start
        time.sleep(5)

        # find analyst containers (names start with 'analyst-')
        try: 
            analyst_containers = find_analysts_containers(docker_cli)
        except Exception as e:
            print(f"ERROR: {e}")
            success = False
            return
        
        # wait for each analyst to exit and check code
        for c in analyst_containers:
            print(f"Waiting for {c.name} to stop...")
            wait_for_exit(c, timeout=120)

            c.reload()
            state = c.attrs.get('State', {})
            exit_code = state.get('ExitCode')
            print(f"Container {c.name} state: {state.get('Status')}, exit code: {exit_code}")
            if exit_code is None:
                print(f"ERROR: could not determine exit code for {c.name}")
                success = False
                return
            elif exit_code != 0:
                print(f"ERROR: analyst {c.name} exited with code {exit_code}")
                success = False
                return

        try:
            check_system_still_running(docker_cli)
        except Exception as e:
            print(f"ERROR: {e}")
            success = False
            return

    finally:
        run_compose_down()

    if success:
        print("SUCCESS: all checks passed")
        sys.exit(0)
    else:
        print("FAILURE: one or more checks failed")
        sys.exit(1)


if __name__ == '__main__':
    test_simultaneous_analysts()
    subprocess.call(['docker', 'compose', 'down', '--volumes', '--remove-orphans'], cwd=REPO_ROOT)

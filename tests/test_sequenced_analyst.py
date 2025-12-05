import os
import subprocess
import time
import sys
import importlib.util
import docker

# Ensure repository root is on sys.path when running this script directly so
# `from tests.utils import *` resolves.
repo_root = os.path.dirname(os.path.dirname(__file__))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from tests.utils import *

def test_sequenced_analyst():
    generate_compose_with_analysts(1)

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

        try:
            c = find_analysts_containers(docker_cli)[0]
        except Exception as e:
            print(f"ERROR: {e}")
            success = False
            return

        print(f"Found analyst container: {c.name} (id={c.id})")

        # wait for first run to exit
        print("Waiting for first run to exit...")
        exit_code1, state1 = wait_for_exit(c, timeout=3600)
        print(f"First run state: {state1}, exit_code={exit_code1}")
        if exit_code1 is None:
            print("ERROR: analyst did not exit within timeout (first run)")
            success = False
            return
        elif exit_code1 != 0:
            print(f"ERROR: analyst first run exited with code {exit_code1}")
            success = False
            return

        try:
            check_system_still_running(docker_cli)
        except Exception as e:
            print(f"ERROR: {e}")
            success = False
            return

        # restart the analyst container
        print("Restarting analyst container...")
        try:
            c.restart()
        except Exception as e:
            print(f"ERROR: failed to restart container: {e}")
            success = False
            return

        # wait for second run to exit
        print("Waiting for second run to exit...")
        exit_code2, state2 = wait_for_exit(c, timeout=3600)
        print(f"Second run state: {state2}, exit_code={exit_code2}")
        if exit_code2 is None:
            print("ERROR: analyst did not exit within timeout (second run)")
            success = False
            return
        elif exit_code2 != 0:
            print(f"ERROR: analyst second run exited with code {exit_code2}")
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
        print("SUCCESS: both runs exited with code 0")
        sys.exit(0)
    else:
        print("FAILURE: one or more checks failed")
        sys.exit(1)


if __name__ == '__main__':
    test_sequenced_analyst()

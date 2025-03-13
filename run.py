import subprocess
import sys
import time
import shlex


def run_script(command):
    print(f"\n{'=' * 50}")
    print(f"Starting {command}...")
    print(f"{'=' * 50}\n")

    # Split the command into parts (script name and arguments)
    command_parts = shlex.split(command)
    script_name = command_parts[0]

    try:
        result = subprocess.run(
            [sys.executable] + command_parts, check=True, text=True, capture_output=True
        )
        print(result.stdout)
        if result.stderr:
            print(f"Warnings/Errors:\n{result.stderr}")
        print(f"\n✅ {command} completed successfully.\n")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running {command}:")
        print(f"Exit code: {e.returncode}")
        print(f"Output: {e.stdout}")
        print(f"Error: {e.stderr}")
        return False


def main():
    scripts = [
        "business_analyst.py",
        "integration_test_spec.py",
        "implementation_planner.py",
        "sql_tests.py",
        "implementation_executor.py",
        "document_process.py",
    ]

    for script in scripts:
        success = run_script(script)
        if not success:
            print(f"Pipeline stopped due to error in {script}")
            break
        time.sleep(1)  # Small pause between scripts

    print("\nPipeline execution completed.")


if __name__ == "__main__":
    main()

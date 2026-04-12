import subprocess
import sys
import os

def run_command(command, cwd=None):
    print(f"\n>>> Running: {command} <<<")
    # Using sys.executable allows us to stay in the same environment for python scripts
    if "python" in command.lower() and not command.startswith(sys.executable):
         command = command.replace("python", f'"{sys.executable}"')
    
    # Check if we are running a .py file directly and prepend python if so
    if command.endswith(".py") and not command.startswith('"'):
        command = f'"{sys.executable}" {command}'

    try:
        # run with check=True to raise CalledProcessError on failure
        # capture_output=False (default) results in streaming to current terminal
        result = subprocess.run(command, shell=True, cwd=cwd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"\n[!] FAILED: command '{command}' failed with exit code {e.returncode}")
        if "dbt" in command:
            print("[TIP] It looks like dbt failed. Ensure dbt-duckdb is installed and your profile is configured.")
        sys.exit(1)
    except FileNotFoundError:
        print(f"\n[!] FAILED: command '{command}' not found.")
        sys.exit(1)

# --- EXECUTION ---

# 1. Run Python Ingestion
# Using the project root for pathing consistency
ingest_script = os.path.join("ingest", "fetch_data.py")
run_command(ingest_script)

# 2. Run dbt Build (includes Freshness, Run, and Test)
run_command("dbt build", cwd="transform")

print("\n--- Pipeline Completed Successfully! ---")
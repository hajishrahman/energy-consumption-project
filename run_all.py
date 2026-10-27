import subprocess
import sys

steps = [
    "src/ingest.py",
    "src/silver.py",
    "src/process.py",
    "src/predict.py",
    "src/dashboard.py"
]

print(" Starting Energy Pipeline...\n")

for step in steps:
    print(f" Running {step}...")
    result = subprocess.run([sys.executable, step], capture_output=False)
    
    if result.returncode != 0:
        print(f"\n Error in {step}. Stopping pipeline.")
        sys.exit(1)
        
print("\n Pipeline completed successfully!")
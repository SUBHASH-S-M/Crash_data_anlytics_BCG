import json

# Path to the JSON file
base_path="/Users/ssm7/IdeaProjects/case_studies_anlysis/"
file_path = "configs/driver.json"

# Read the JSON file
with open(base_path+file_path, "r") as file:
    data = json.load(file)

# Analyze the keys and values
for key, value in data.items():
    print(f"Key: {key}")
    print(f"Value: {value}")
    print("---")
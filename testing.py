import subprocess

def get_commit_ID():
    try:
        commit_id=subprocess.check_output(["git", "log", "-1", "--format%H"]).strip().decode("utf-8")
        return commit_id
    except subprocess.CalledProcessError as err:
        print("Error while fetching commit ID", err)
        return None

result = get_commit_ID()
print(f"The commit id:{result}")
#### DP3: Github Events Analysis 

<small> Note: .env file used for configuration is excluded from this repository </small>

Repositories polled from GitHub's REST API: 

    "torvalds/linux",
    "microsoft/vscode",
    "facebook/react",
    "pytorch/pytorch",
    "kubernetes/kubernetes",
    "vercel/next.js",
    "flutter/flutter",
    "apache/spark",
    "ansible/ansible",
    "godotengine/godot"

#### Setup Instructions

- ensure you have a local `.env` file in our workspace containing your Github PAT and AWS credentials to connect to a local S3 bucket.
- Create an S3 bucket in your AWS account.
- Create a virtual environment and install the packages from `requirements.txt` using the command:
```bash
pip install -r requirements.txt
```
- Create and run a local prefect server using `prefect server start` command in the terminal. 
- Run the prefect flow (`flows/fullflow.py`) and add a scheduler, whether through the terminal or the prefect server UI.
- Run the Shiny app using the terminal command `shiny run --reload app/app.py`

import time
import random
import schedule
import networkx as nx
import smtplib
from email.mime.text import MIMEText
from functools import wraps
from datetime import datetime


# ================================
# Retry Decorator (Exponential Backoff + Jitter)
# ================================
def retry(max_retries=3, base_delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    wait = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    print(f"Retrying {func.__name__}... attempt {attempt+1}, waiting {wait:.2f}s")
                    time.sleep(wait)
            raise Exception(f"{func.__name__} failed after retries")
        return wrapper
    return decorator


# ================================
# Failure Detection + Email Alert
# ================================
def send_email_alert(error_msg):
    sender = "your_email@gmail.com"
    receiver = "receiver_email@gmail.com"
    password = "your_app_password"  # Gmail App Password

    msg = MIMEText(error_msg)
    msg["Subject"] = "ETL Pipeline Failure"
    msg["From"] = sender
    msg["To"] = receiver

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.send_message(msg)
    except Exception:
        print("Email alert skipped (demo mode)")


# ================================
# Task Definition (DAG Node)
# ================================
class Task:
    def __init__(self, name, func, dependencies=None):
        self.name = name
        self.func = func
        self.dependencies = dependencies or []

    def run(self):
        print(f"Running task: {self.name}")
        self.func()


# ================================
# DAG Orchestration + Dependency Management
# ================================
class PipelineDAG:
    def __init__(self):
        self.graph = nx.DiGraph()

    def add_task(self, task):
        self.graph.add_node(task.name, task=task)
        for dep in task.dependencies:
            self.graph.add_edge(dep, task.name)

    def execute(self):
        try:
            order = list(nx.topological_sort(self.graph))
            print("Execution Order:", order)
            for task_name in order:
                task = self.graph.nodes[task_name]["task"]
                task.run()
        except Exception as e:
            send_email_alert(str(e))
            raise


# ================================
# ETL Tasks (With Retry)
# ================================
@retry()
def extract():
    if random.random() < 0.3:
        raise Exception("Extraction failed")
    print("Extracting data...")

@retry()
def transform():
    print("Transforming data...")

@retry()
def load():
    print("Loading data...")


# ================================
# Build DAG
# ================================
pipeline = PipelineDAG()
pipeline.add_task(Task("extract", extract))
pipeline.add_task(Task("transform", transform, ["extract"]))
pipeline.add_task(Task("load", load, ["transform"]))


# ================================
# Pipeline Runner
# ================================
def run_pipeline():
    print("\nPipeline started at", datetime.now())
    pipeline.execute()
    print("Pipeline completed!\n")


# ================================
# Scheduler (Every 1 Minute)
# ================================
schedule.every(1).minutes.do(run_pipeline)

print("Scheduler started... Press Ctrl+C to stop.")

while True:
    schedule.run_pending()
    time.sleep(1)

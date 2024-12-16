from airflow import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago

# Define the Slack webhook URL (replace with your actual webhook URL)
SLACK_WEBHOOK_URL = "https://app.slack.com/client/T083UK72JR5/C083UKB1KB9"

# Define the DAG and its default arguments
dag = DAG(
    'slack_notification_example',
    default_args={
        'owner': 'airflow',
    },
    description='DAG with Slack notifications',
    schedule_interval='@daily',
    start_date=days_ago(1),
)

# Slack message task
slack_message = SlackWebhookOperator(
    task_id='send_slack_message',
    http_conn_id=None,  # Slack Webhook URL doesn't need a connection ID
    webhook_token=SLACK_WEBHOOK_URL,  # Slack Webhook URL
    message="Hello, Airflow!",  # Message to send
    channel="#general",  # Slack channel to send message to
    dag=dag,
)

# Set the task to execute
slack_message
